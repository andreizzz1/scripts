from __future__ import annotations

import asyncpg
from dataclasses import dataclass
from typing import Optional

import asyncpg.exceptions

from .config import AppConfig, FeatureToggles
from .domain import DickRow, GrowthResult, UserRow


@dataclass(frozen=True)
class ChatIdKind:
    kind: str  # "id" | "inst"
    value: str

    @classmethod
    def from_chat_id(cls, chat_id: int) -> "ChatIdKind":
        return cls(kind="id", value=str(chat_id))

    @classmethod
    def from_chat_instance(cls, chat_instance: str) -> "ChatIdKind":
        return cls(kind="inst", value=chat_instance)


@dataclass(frozen=True)
class ChatIdFull:
    chat_id: int
    chat_instance: str


@dataclass(frozen=True)
class ChatIdPartiality:
    full: Optional[ChatIdFull] = None
    specific: Optional[ChatIdKind] = None
    source: str = "database"  # "database" | "inline"

    @classmethod
    def from_chat_id(cls, chat_id: int) -> "ChatIdPartiality":
        return cls(specific=ChatIdKind.from_chat_id(chat_id))

    @classmethod
    def from_chat_instance(cls, chat_instance: str) -> "ChatIdPartiality":
        return cls(specific=ChatIdKind.from_chat_instance(chat_instance))

    def kind(self, chats_merging: bool) -> ChatIdKind:
        if self.specific:
            return self.specific
        assert self.full is not None
        if chats_merging:
            # prefer numeric chat_id when possible
            return ChatIdKind.from_chat_id(self.full.chat_id)
        if self.source == "inline":
            return ChatIdKind.from_chat_instance(self.full.chat_instance)
        return ChatIdKind.from_chat_id(self.full.chat_id)


class UsersRepo:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def create_or_update(self, uid: int, name: str) -> UserRow:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO Users(uid, name) VALUES ($1, $2)
                ON CONFLICT (uid) DO UPDATE SET name = $2
                RETURNING uid, name, created_at
                """,
                uid,
                name,
            )
            assert row is not None
            return UserRow(uid=row["uid"], name=row["name"], created_at=row["created_at"])

    async def get_random_active_member(self, chat: ChatIdKind) -> Optional[UserRow]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT u.uid, u.name, u.created_at
                FROM Users u
                  JOIN Dicks d USING (uid)
                  JOIN Chats c ON d.chat_id = c.id
                WHERE (c.chat_id::text = $1 OR c.chat_instance = $1)
                  AND d.updated_at > current_timestamp - interval '1 week'
                ORDER BY random() LIMIT 1
                """,
                chat.value,
            )
            if row is None:
                return None
            return UserRow(uid=row["uid"], name=row["name"], created_at=row["created_at"])

    async def get_random_active_poor_member(self, chat: ChatIdKind, rich_exclusion_ratio: float) -> Optional[UserRow]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH ranked_users AS (
                    SELECT u.uid, u.name, u.created_at, PERCENT_RANK() OVER (ORDER BY d.length) AS percentile_rank
                    FROM Users u
                      JOIN Dicks d USING (uid)
                      JOIN Chats c ON d.chat_id = c.id
                    WHERE (c.chat_id::text = $1 OR c.chat_instance = $1)
                      AND d.updated_at > current_timestamp - interval '1 week'
                )
                SELECT uid, name, created_at
                FROM ranked_users
                WHERE percentile_rank <= $2
                ORDER BY random()
                LIMIT 1
                """,
                chat.value,
                1.0 - rich_exclusion_ratio,
            )
            if row is None:
                return None
            return UserRow(uid=row["uid"], name=row["name"], created_at=row["created_at"])

    async def get_random_active_member_with_poor_in_priority(self, chat: ChatIdKind) -> Optional[UserRow]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH user_weights AS (
                    SELECT u.uid, u.name, u.created_at, d.length,
                           1.0 / (1.0 + EXP(d.length / 6.0)) AS weight
                    FROM Users u
                      JOIN Dicks d USING (uid)
                      JOIN Chats c ON d.chat_id = c.id
                    WHERE (c.chat_id::text = $1 OR c.chat_instance = $1)
                      AND d.updated_at > current_timestamp - interval '1 week'
                ),
                cumulative_weights AS (
                    SELECT uid, name, created_at, weight,
                           SUM(weight) OVER (ORDER BY uid) AS cumulative_weight,
                           SUM(weight) OVER () AS total_weight
                    FROM user_weights
                ),
                random_value AS (
                    SELECT RANDOM() * (SELECT total_weight FROM cumulative_weights LIMIT 1) AS rand_value
                )
                SELECT uid, name, created_at
                FROM cumulative_weights, random_value
                WHERE cumulative_weight >= random_value.rand_value
                ORDER BY cumulative_weight
                LIMIT 1
                """,
                chat.value,
            )
            if row is None:
                return None
            return UserRow(uid=row["uid"], name=row["name"], created_at=row["created_at"])

    async def get(self, uid: int) -> Optional[UserRow]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT uid, name, created_at FROM Users WHERE uid = $1", uid)
            if row is None:
                return None
            return UserRow(uid=row["uid"], name=row["name"], created_at=row["created_at"])

    async def get_chat_members(self, chat: ChatIdKind) -> list[UserRow]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT u.uid, u.name, u.created_at
                FROM Users u
                  JOIN Dicks d USING (uid)
                  JOIN Chats c ON d.chat_id = c.id
                WHERE c.chat_id = $1::bigint OR c.chat_instance = $1::text
                """,
                chat.value,
            )
            return [UserRow(uid=r["uid"], name=r["name"], created_at=r["created_at"]) for r in rows]


class ChatsRepo:
    def __init__(self, pool: asyncpg.Pool, features: FeatureToggles):
        self._pool = pool
        self._features = features

    async def upsert_chat(self, chat: ChatIdPartiality) -> int:
        chats_merging = self._features.chats_merging
        if chat.specific:
            if chat.specific.kind == "id":
                cid = int(chat.specific.value)
                inst = None
            else:
                cid = None
                inst = chat.specific.value
        else:
            assert chat.full is not None
            if chats_merging:
                cid = chat.full.chat_id
                inst = chat.full.chat_instance
            elif chat.source == "inline":
                cid = None
                inst = chat.full.chat_instance
            else:
                cid = chat.full.chat_id
                inst = None

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    "SELECT id, chat_id, chat_instance FROM Chats WHERE chat_id = $1 OR chat_instance = $2",
                    cid,
                    inst,
                )
                if len(rows) == 1:
                    r = rows[0]
                    if r["chat_id"] == cid and r["chat_instance"] == inst:
                        return int(r["id"])
                    row = await conn.fetchrow(
                        """
                        UPDATE Chats
                        SET chat_id = coalesce($2, chat_id),
                            chat_instance = coalesce($3, chat_instance)
                        WHERE id = $1
                        RETURNING id
                        """,
                        r["id"],
                        cid,
                        inst,
                    )
                    assert row is not None
                    return int(row["id"])

                if len(rows) == 0:
                    row = await conn.fetchrow(
                        "INSERT INTO Chats (chat_id, chat_instance) VALUES ($1, $2) RETURNING id",
                        cid,
                        inst,
                    )
                    assert row is not None
                    return int(row["id"])

                if len(rows) == 2 and chats_merging:
                    # Merge chats: keep one that has chat_id, delete the other that has chat_instance.
                    a, b = rows[0], rows[1]
                    chat_id_row = a if a["chat_id"] is not None else b
                    inst_row = a if a["chat_instance"] is not None else b
                    main_id = int(chat_id_row["id"])
                    deleted_id = int(inst_row["id"])
                    main_chat_id = int(chat_id_row["chat_id"])
                    main_inst = str(inst_row["chat_instance"])

                    # sum duplicates by uid, then move lengths to main chat with +1 bonus_attempt
                    await conn.execute(
                        """
                        WITH sum_dicks AS (
                          SELECT uid, SUM(length) AS length
                          FROM Dicks
                          WHERE chat_id = $1 OR chat_id = $2
                          GROUP BY uid
                        )
                        UPDATE Dicks d
                        SET length = sum_dicks.length,
                            chat_id = $1,
                            bonus_attempts = (bonus_attempts + 1)
                        FROM sum_dicks
                        WHERE d.chat_id = $1 AND d.uid = sum_dicks.uid
                        """,
                        main_id,
                        deleted_id,
                    )
                    await conn.execute("DELETE FROM Dicks WHERE chat_id = $1", deleted_id)
                    await conn.execute(
                        "DELETE FROM Chats WHERE id = $1 AND chat_instance = $2",
                        deleted_id,
                        main_inst,
                    )
                    await conn.execute(
                        "UPDATE Chats SET chat_instance = $3 WHERE id = $1 AND chat_id = $2",
                        main_id,
                        main_chat_id,
                        main_inst,
                    )
                    return main_id

                raise RuntimeError(f"unexpected Chats match count ({len(rows)}) for {chat}")

    async def get_internal_id(self, chat: ChatIdKind) -> int:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM Chats WHERE chat_id::text = $1 OR chat_instance = $1",
                chat.value,
            )
            if row is None:
                raise KeyError(chat.value)
            return int(row["id"])


class DicksRepo:
    def __init__(self, pool: asyncpg.Pool, features: FeatureToggles):
        self._pool = pool
        self._features = features
        self._chats = ChatsRepo(pool, features)

    async def create_or_grow(self, uid: int, chat: ChatIdPartiality, increment: int) -> GrowthResult:
        internal_chat_id = await self._chats.upsert_chat(chat)
        async with self._pool.acquire() as conn:
            new_length = await conn.fetchval(
                """
                INSERT INTO dicks(uid, chat_id, length, updated_at)
                VALUES ($1, $2, $3, current_timestamp)
                ON CONFLICT (uid, chat_id)
                DO UPDATE SET length = (dicks.length + $3), updated_at = current_timestamp
                RETURNING length
                """,
                uid,
                internal_chat_id,
                increment,
            )
            pos = await self._position_in_top(conn, internal_chat_id, uid)
            return GrowthResult(new_length=int(new_length), pos_in_top=pos)

    async def _position_in_top(self, conn: asyncpg.Connection, chat_internal_id: int, uid: int) -> Optional[int]:
        if not self._features.top_unlimited:
            return None
        pos = await conn.fetchval(
            """
            SELECT position FROM (
                SELECT uid, ROW_NUMBER() OVER (ORDER BY length DESC, updated_at DESC, name) AS position
                FROM dicks
                  JOIN users USING (uid)
                WHERE chat_id = $1
            ) AS _
            WHERE uid = $2
            """,
            chat_internal_id,
            uid,
        )
        return int(pos) if pos is not None else None

    async def fetch_length(self, uid: int, chat: ChatIdKind) -> int:
        async with self._pool.acquire() as conn:
            length = await conn.fetchval(
                """
                SELECT d.length
                FROM Dicks d
                  JOIN Chats c ON d.chat_id = c.id
                WHERE uid = $1 AND (c.chat_id::text = $2 OR c.chat_instance = $2)
                """,
                uid,
                chat.value,
            )
            return int(length or 0)

    async def fetch_dick(self, uid: int, chat: ChatIdKind) -> Optional[DickRow]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT length, uid as owner_uid, name as owner_name, updated_at as grown_at, position
                FROM (
                    SELECT uid, name, d.length AS length, updated_at,
                           ROW_NUMBER() OVER (ORDER BY length DESC, updated_at DESC, name) AS position
                    FROM Dicks d
                      JOIN users USING (uid)
                      JOIN Chats c ON d.chat_id = c.id
                    WHERE c.chat_id::text = $2 OR c.chat_instance = $2
                ) AS _
                WHERE uid = $1
                """,
                uid,
                chat.value,
            )
            if row is None:
                return None
            return DickRow(
                length=int(row["length"]),
                owner_uid=int(row["owner_uid"]),
                owner_name=str(row["owner_name"]),
                grown_at=row["grown_at"],
                position=int(row["position"]) if row["position"] is not None else None,
            )

    async def get_top(self, chat: ChatIdKind, offset: int, limit: int) -> list[DickRow]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT length, uid as owner_uid, name as owner_name, updated_at as grown_at,
                       ROW_NUMBER() OVER (ORDER BY length DESC, updated_at DESC, name) AS position
                FROM dicks d
                  JOIN users USING (uid)
                  JOIN chats c ON c.id = d.chat_id
                WHERE c.chat_id::text = $1 OR c.chat_instance = $1
                OFFSET $2 LIMIT $3
                """,
                chat.value,
                offset,
                limit,
            )
            return [
                DickRow(
                    length=int(r["length"]),
                    owner_uid=int(r["owner_uid"]),
                    owner_name=str(r["owner_name"]),
                    grown_at=r["grown_at"],
                    position=int(r["position"]) if r["position"] is not None else None,
                )
                for r in rows
            ]

    async def set_dod_winner(self, chat: ChatIdPartiality, uid: int, bonus: int) -> Optional[GrowthResult]:
        internal_chat_id = await self._chats.upsert_chat(chat)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                new_length = await conn.fetchval(
                    """
                    UPDATE Dicks
                    SET bonus_attempts = (bonus_attempts + 1), length = (length + $3)
                    WHERE chat_id = $1 AND uid = $2
                    RETURNING length
                    """,
                    internal_chat_id,
                    uid,
                    bonus,
                )
                if new_length is None:
                    return None
                await conn.execute(
                    "INSERT INTO Dick_of_Day (chat_id, winner_uid) VALUES ($1, $2)",
                    internal_chat_id,
                    uid,
                )
            pos = await self._position_in_top(conn, internal_chat_id, uid)
            return GrowthResult(new_length=int(new_length), pos_in_top=pos)

    async def grow_no_attempts_check(self, chat: ChatIdKind, uid: int, change: int) -> GrowthResult:
        internal_chat_id = await self._chats.get_internal_id(chat)
        async with self._pool.acquire() as conn:
            new_length = await conn.fetchval(
                """
                UPDATE Dicks
                SET bonus_attempts = (bonus_attempts + 1),
                    length = (length + $3)
                WHERE chat_id = $1 AND uid = $2
                RETURNING length
                """,
                internal_chat_id,
                uid,
                change,
            )
            if new_length is None:
                raise KeyError(f"missing dick for uid={uid} chat={chat.value}")
            pos = await self._position_in_top(conn, internal_chat_id, uid)
            return GrowthResult(new_length=int(new_length), pos_in_top=pos)

    async def check_dick(self, chat: ChatIdKind, uid: int, length: int) -> bool:
        async with self._pool.acquire() as conn:
            enough = await conn.fetchval(
                """
                SELECT length >= $3 AS enough
                FROM Dicks d
                  JOIN Chats c ON d.chat_id = c.id
                WHERE (c.chat_id::text = $1 OR c.chat_instance = $1) AND uid = $2
                """,
                chat.value,
                uid,
                length,
            )
            return bool(enough or False)

    async def move_length(self, chat: ChatIdPartiality, from_uid: int, to_uid: int, length: int) -> tuple[GrowthResult, GrowthResult]:
        internal_chat_id = await self._chats.upsert_chat(chat)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                length_from = await conn.fetchval(
                    """
                    UPDATE Dicks
                    SET length = (length + $3), bonus_attempts = (bonus_attempts + 1)
                    WHERE chat_id = $1 AND uid = $2
                    RETURNING length
                    """,
                    internal_chat_id,
                    from_uid,
                    -length,
                )
                length_to = await conn.fetchval(
                    """
                    UPDATE Dicks
                    SET length = (length + $3), bonus_attempts = (bonus_attempts + 1)
                    WHERE chat_id = $1 AND uid = $2
                    RETURNING length
                    """,
                    internal_chat_id,
                    to_uid,
                    length,
                )
            pos_from = await self._position_in_top(conn, internal_chat_id, from_uid)
            pos_to = await self._position_in_top(conn, internal_chat_id, to_uid)
            return (
                GrowthResult(new_length=int(length_from), pos_in_top=pos_from),
                GrowthResult(new_length=int(length_to), pos_in_top=pos_to),
            )


@dataclass(frozen=True)
class Loan:
    debt: int
    payout_ratio: float


class LoansRepo:
    def __init__(self, pool: asyncpg.Pool, cfg: AppConfig):
        self._pool = pool
        self._cfg = cfg

    async def get_active_loan(self, uid: int, chat: ChatIdKind) -> Optional[Loan]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT l.debt, l.payout_ratio
                FROM Loans l
                  JOIN Chats c ON l.chat_id = c.id
                WHERE l.uid = $1 AND l.repaid_at IS NULL
                  AND (c.chat_id::text = $2 OR c.chat_instance = $2)
                ORDER BY l.id DESC
                LIMIT 1
                """,
                uid,
                chat.value,
            )
            if row is None:
                return None
            return Loan(debt=int(row["debt"]), payout_ratio=float(row["payout_ratio"]))

    async def borrow(self, uid: int, chat: ChatIdKind, value: int) -> None:
        async with self._pool.acquire() as conn:
            internal_chat_id = await ChatsRepo(self._pool, self._cfg.features).get_internal_id(chat)
            await conn.execute(
                "INSERT INTO Loans(uid, chat_id, debt, payout_ratio) VALUES ($1, $2, $3, $4)",
                uid,
                internal_chat_id,
                value,
                self._cfg.loan_payout_ratio,
            )
            # reset length to 0 and grant an extra attempt
            await conn.execute(
                """
                UPDATE Dicks
                SET length = 0, bonus_attempts = (bonus_attempts + 1)
                WHERE uid = $1 AND chat_id = $2
                """,
                uid,
                internal_chat_id,
            )

    async def pay(self, uid: int, chat: ChatIdKind, payout: int) -> None:
        if payout <= 0:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE Loans l
                SET debt = GREATEST(debt - $3, 0)
                FROM Chats c
                WHERE l.chat_id = c.id
                  AND l.uid = $1
                  AND l.repaid_at IS NULL
                  AND (c.chat_id::text = $2 OR c.chat_instance = $2)
                """,
                uid,
                chat.value,
                payout,
            )


@dataclass(frozen=True)
class UserPvpStats:
    battles_total: int = 0
    battles_won: int = 0
    win_streak_max: int = 0
    win_streak_current: int = 0
    acquired_length: int = 0
    lost_length: int = 0

    def win_rate_percentage(self) -> float:
        if self.battles_total <= 0:
            return 0.0
        return (self.battles_won / self.battles_total) * 100.0

    def win_rate_formatted(self) -> str:
        return f"{self.win_rate_percentage():.2f}%"


class BattleStatsRepo:
    def __init__(self, pool: asyncpg.Pool, features: FeatureToggles):
        self._pool = pool
        self._chats = ChatsRepo(pool, features)

    async def get_stats(self, chat: ChatIdKind, uid: int) -> UserPvpStats:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT battles_total, battles_won, win_streak_max, win_streak_current, acquired_length, lost_length
                FROM Battle_Stats
                WHERE chat_id = (SELECT id FROM Chats WHERE chat_id::text = $1 OR chat_instance = $1)
                  AND uid = $2
                """,
                chat.value,
                uid,
            )
            if row is None:
                return UserPvpStats()
            return UserPvpStats(
                battles_total=int(row["battles_total"]),
                battles_won=int(row["battles_won"]),
                win_streak_max=int(row["win_streak_max"]),
                win_streak_current=int(row["win_streak_current"]),
                acquired_length=int(row["acquired_length"]),
                lost_length=int(row["lost_length"]),
            )

    async def send_battle_result(self, chat: ChatIdKind, winner_uid: int, loser_uid: int, bet: int) -> tuple[UserPvpStats, float, int]:
        """
        Updates Battle_Stats for winner/loser.
        Returns: (winner_stats, loser_win_rate_percentage, loser_prev_win_streak).
        """
        chat_internal_id = await self._chats.get_internal_id(chat)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                winner_row = await conn.fetchrow(
                    """
                    INSERT INTO Battle_Stats(uid, chat_id, battles_total, battles_won, win_streak_current, acquired_length)
                    VALUES ($1, $2, 1, 1, 1, $3)
                    ON CONFLICT (uid, chat_id) DO UPDATE SET
                      battles_total = Battle_Stats.battles_total + 1,
                      battles_won = Battle_Stats.battles_won + 1,
                      win_streak_current = Battle_Stats.win_streak_current + 1,
                      acquired_length = Battle_Stats.acquired_length + $3
                    RETURNING battles_total, battles_won, win_streak_max, win_streak_current, acquired_length, lost_length
                    """,
                    winner_uid,
                    chat_internal_id,
                    bet,
                )
                assert winner_row is not None

                prev_streak = await conn.fetchval(
                    "SELECT win_streak_current FROM Battle_Stats WHERE chat_id = $1 AND uid = $2",
                    chat_internal_id,
                    loser_uid,
                )
                prev_streak = int(prev_streak or 0)

                loser_row = await conn.fetchrow(
                    """
                    INSERT INTO Battle_Stats(uid, chat_id, battles_total, battles_won, win_streak_current, lost_length)
                    VALUES ($1, $2, 1, 0, 0, $3)
                    ON CONFLICT (uid, chat_id) DO UPDATE SET
                      battles_total = Battle_Stats.battles_total + 1,
                      win_streak_current = 0,
                      lost_length = Battle_Stats.lost_length + $3
                    RETURNING battles_total, battles_won
                    """,
                    loser_uid,
                    chat_internal_id,
                    bet,
                )
                assert loser_row is not None

        winner_stats = UserPvpStats(
            battles_total=int(winner_row["battles_total"]),
            battles_won=int(winner_row["battles_won"]),
            win_streak_max=int(winner_row["win_streak_max"]),
            win_streak_current=int(winner_row["win_streak_current"]),
            acquired_length=int(winner_row["acquired_length"]),
            lost_length=int(winner_row["lost_length"]),
        )
        loser_total = int(loser_row["battles_total"])
        loser_won = int(loser_row["battles_won"])
        loser_wr = 0.0 if loser_total <= 0 else (loser_won / loser_total) * 100.0
        return winner_stats, loser_wr, prev_streak


@dataclass(frozen=True)
class PersonalStats:
    chats: int
    max_length: int
    total_length: int


class PersonalStatsRepo:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get(self, uid: int) -> PersonalStats:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT count(chat_id) AS chats,
                       max(length) AS max_length,
                       sum(length) AS total_length
                FROM Dicks
                WHERE uid = $1
                """,
                uid,
            )
            assert row is not None
            return PersonalStats(
                chats=int(row["chats"] or 0),
                max_length=int(row["max_length"] or 0),
                total_length=int(row["total_length"] or 0),
            )


@dataclass(frozen=True)
class PromoActivationResult:
    chats_affected: int
    bonus_length: int


class PromoActivationError(Exception):
    code: str

    def __init__(self, code: str, message: str = ""):
        super().__init__(message or code)
        self.code = code


class PromoRepo:
    PROMOCODE_ACTIVATIONS_PK = "promo_code_activations_pkey"

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def activate(self, uid: int, code: str) -> PromoActivationResult:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    UPDATE Promo_Codes
                    SET capacity = (capacity - 1)
                    WHERE lower(code) = lower($1)
                      AND capacity > 0
                      AND (
                        current_date BETWEEN since AND until
                        OR (current_date >= since AND until IS NULL)
                      )
                    RETURNING bonus_length, code AS found_code
                    """,
                    code,
                )
                if row is None:
                    raise PromoActivationError("no_activations_left")
                bonus_length = int(row["bonus_length"])
                found_code = str(row["found_code"])

                updated = await conn.execute(
                    """
                    UPDATE Dicks
                    SET bonus_attempts = (bonus_attempts + 1),
                        length = (length + $2)
                    WHERE uid = $1
                    """,
                    uid,
                    bonus_length,
                )
                # "UPDATE X" -> parse affected count
                affected = int(updated.split()[-1])
                if affected < 1:
                    raise PromoActivationError("no_dicks")

                try:
                    await conn.execute(
                        """
                        INSERT INTO Promo_Code_Activations (uid, code, affected_chats)
                        VALUES ($1, $2, $3)
                        """,
                        uid,
                        found_code,
                        affected,
                    )
                except asyncpg.exceptions.UniqueViolationError as e:
                    if getattr(e, "constraint_name", None) == self.PROMOCODE_ACTIVATIONS_PK:
                        raise PromoActivationError("already_activated") from e
                    raise
                return PromoActivationResult(chats_affected=affected, bonus_length=bonus_length)


@dataclass(frozen=True)
class ExternalUser:
    uid: int
    length: int


class ImportRepo:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_imported_users(self, chat_id: int) -> list[ExternalUser]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT uid, original_length AS length FROM Imports WHERE chat_id = $1",
                chat_id,
            )
            return [ExternalUser(uid=int(r["uid"]), length=int(r["length"])) for r in rows]

    async def import_users(self, chat_id: int, users: list[ExternalUser]) -> None:
        if not users:
            return
        uids = [u.uid for u in users]
        lengths = [u.length for u in users]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO Imports (chat_id, uid, original_length)
                    SELECT $1, * FROM UNNEST($2::bigint[], $3::int[])
                    """,
                    chat_id,
                    uids,
                    lengths,
                )
                await conn.execute(
                    """
                    WITH original AS (
                      SELECT c.id AS chat_id, uid, original_length
                      FROM Imports
                        JOIN Chats c USING (chat_id)
                      WHERE chat_id = $1 AND uid = ANY($2)
                    )
                    UPDATE Dicks d
                    SET length = (length + original_length),
                        bonus_attempts = (bonus_attempts + 1)
                    FROM original o
                    WHERE d.chat_id = o.chat_id AND d.uid = o.uid
                    """,
                    chat_id,
                    uids,
                )


@dataclass(frozen=True)
class Repositories:
    users: UsersRepo
    chats: ChatsRepo
    dicks: DicksRepo
    loans: LoansRepo
    pvp_stats: BattleStatsRepo
    personal_stats: PersonalStatsRepo
    promo: PromoRepo
    import_repo: ImportRepo

    @classmethod
    def create(cls, pool: asyncpg.Pool, cfg: AppConfig) -> "Repositories":
        users = UsersRepo(pool)
        chats = ChatsRepo(pool, cfg.features)
        dicks = DicksRepo(pool, cfg.features)
        loans = LoansRepo(pool, cfg)
        pvp_stats = BattleStatsRepo(pool, cfg.features)
        personal_stats = PersonalStatsRepo(pool)
        promo = PromoRepo(pool)
        import_repo = ImportRepo(pool)
        return cls(
            users=users,
            chats=chats,
            dicks=dicks,
            loans=loans,
            pvp_stats=pvp_stats,
            personal_stats=personal_stats,
            promo=promo,
            import_repo=import_repo,
        )
