from __future__ import annotations

import base64
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from dotenv import load_dotenv

from .config import AppConfig, DickOfDaySelectionMode, load_config
from .db import Database, apply_sql_migrations
from .domain import escape_html, normalize_locale
from .help_content import load_privacy_policy, render_help_messages
from .i18n import I18n
from .incrementor import Incrementor
from .perks import HelpPussiesPerk, LoanPayoutPerk
from .repo import ChatIdKind, ChatIdPartiality, PromoActivationError, Repositories
from .repo import ExternalUser
from .utils import get_full_name, time_till_next_day

CALLBACK_PREFIX_TOP_PAGE = "top:page:"
PROMO_START_PARAM_PREFIX = "promo-"
CALLBACK_PREFIX_PVP = "pvp:"
ORIGINAL_BOT_USERNAMES = {"pipisabot", "kraft28_bot"}

# 22.06.2024 UTC in ms
TIMESTAMP_MILLIS_SINCE_2024 = 1719014400000


class PromoFlow(StatesGroup):
    requested = State()


def _decode_deeplink_payload(encoded: str) -> str:
    # URL_SAFE_NO_PAD equivalent
    padding = "=" * (-len(encoded) % 4)
    data = base64.urlsafe_b64decode((encoded + padding).encode("ascii"))
    return data.decode("utf-8")


def _word_chats_ru(count: int) -> str:
    return "чате" if count == 1 else "чатах"

def _convert_name_for_import(original_bot_username: str, full_name: str) -> str:
    if original_bot_username == "pipisabot":
        return full_name[:13]
    return full_name


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"environment variable not found: {key}")
    return value


def _build_top_keyboard(page: int, has_more: bool) -> InlineKeyboardMarkup:
    buttons: list[InlineKeyboardButton] = []
    if page > 0:
        buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"{CALLBACK_PREFIX_TOP_PAGE}{page-1}"))
    if has_more:
        buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"{CALLBACK_PREFIX_TOP_PAGE}{page+1}"))
    return InlineKeyboardMarkup(inline_keyboard=[buttons])


async def _reply_html(message: Message, text: str, *, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup, disable_web_page_preview=True)


router = Router()


@router.message(Command("help"))
async def cmd_help(message: Message, help_container, i18n: I18n) -> None:  # type: ignore[no-untyped-def]
    locale = normalize_locale(getattr(message.from_user, "language_code", None))
    await _reply_html(message, help_container.get_help_message(locale))


@router.message(Command("privacy"))
async def cmd_privacy(message: Message, privacy_policy: dict[str, str]) -> None:
    locale = normalize_locale(getattr(message.from_user, "language_code", None))
    key = "ru" if locale.startswith("ru") else "en"
    await _reply_html(message, privacy_policy[key])


@router.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject, help_container, i18n: I18n, repos: Repositories) -> None:  # type: ignore[no-untyped-def]
    locale = normalize_locale(getattr(message.from_user, "language_code", None))
    greeting = i18n.t("titles.greeting", locale)

    args = (command.args or "").strip()
    if args.startswith(PROMO_START_PARAM_PREFIX):
        encoded = args[len(PROMO_START_PARAM_PREFIX):]
        try:
            promo_code = _decode_deeplink_payload(encoded)
            res = await repos.promo.activate(message.from_user.id, promo_code)
            suffix = "plural" if res.chats_affected > 1 else "singular"
            ending = i18n.t(
                f"commands.promo.success.{suffix}",
                locale,
                growth=res.bonus_length,
                affected_chats=res.chats_affected,
                word_chats=_word_chats_ru(res.chats_affected),
            )
            await _reply_html(message, i18n.t("commands.promo.success.template", locale, ending=ending))
        except PromoActivationError as e:
            await _reply_html(message, i18n.t(f"commands.promo.errors.{e.code}", locale))
        except Exception:
            await _reply_html(message, i18n.t("errors.feature_disabled", locale))
        return

    username = escape_html(getattr(message.from_user, "first_name", ""))
    await _reply_html(message, help_container.get_start_message(username, locale, greeting))


@router.message(Command("grow"))
async def cmd_grow(message: Message, repos: Repositories, incrementor: Incrementor, i18n: I18n, cfg: AppConfig) -> None:
    if message.from_user is None:
        return
    locale = normalize_locale(message.from_user.language_code)
    chat = ChatIdPartiality.from_chat_id(message.chat.id)

    name = get_full_name(message.from_user)
    user = await repos.users.create_or_update(message.from_user.id, name)
    days_since_registration = int((datetime.now(tz=timezone.utc) - user.created_at).total_seconds() // 86400)
    incr = await incrementor.growth_increment(message.from_user.id, chat.kind(cfg.features.chats_merging), days_since_registration)

    try:
        result = await repos.dicks.create_or_grow(message.from_user.id, chat, incr.total)
        event_key = "shrunk" if incr.total < 0 else "grown"
        event = i18n.t(f"commands.grow.direction.{event_key}", locale)
        answer = i18n.t(
            "commands.grow.result",
            locale,
            event=event,
            incr=abs(incr.total),
            length=result.new_length,
        )
        if result.pos_in_top is not None:
            answer = f"{answer}\n{i18n.t('commands.grow.position', locale, pos=result.pos_in_top)}"
    except asyncpg.PostgresError as e:
        if getattr(e, "sqlstate", None) == "GD0E1":
            answer = i18n.t("commands.grow.tomorrow", locale)
        else:
            raise

    answer = f"{answer}{time_till_next_day(i18n, locale)}"
    await _reply_html(message, answer)


async def _render_top(
    *,
    requester_uid: int,
    chat_kind: ChatIdKind,
    page: int,
    repos: Repositories,
    cfg: AppConfig,
    i18n: I18n,
    locale: str,
) -> tuple[str, bool]:
    top_limit = int(cfg.top_limit)
    offset = page * top_limit
    query_limit = top_limit + 1
    dicks = await repos.dicks.get_top(chat_kind, offset=offset, limit=query_limit)
    has_more = len(dicks) > top_limit

    if not dicks:
        return i18n.t("commands.top.empty", locale), False

    now = datetime.now(tz=timezone.utc)
    lines: list[str] = []
    for i, d in enumerate(dicks[:top_limit], start=1):
        escaped_name = escape_html(d.owner_name)
        name = f"<u>{escaped_name}</u>" if d.owner_uid == requester_uid else escaped_name
        can_grow = now.date() > d.grown_at.date()
        pos = d.position or i
        line = i18n.t("commands.top.line", locale, n=pos, name=name, length=d.length)
        if can_grow:
            line += " [+]"
        lines.append(line)

    title = i18n.t("commands.top.title", locale)
    ending = i18n.t("commands.top.ending", locale)
    return f"{title}\n\n" + "\n".join(lines) + f"\n\n{ending}", has_more


@router.message(Command("top"))
async def cmd_top(message: Message, repos: Repositories, cfg: AppConfig, i18n: I18n) -> None:
    if message.from_user is None:
        return
    locale = normalize_locale(message.from_user.language_code)
    chat_kind = ChatIdKind.from_chat_id(message.chat.id)
    text, has_more = await _render_top(
        requester_uid=message.from_user.id,
        chat_kind=chat_kind,
        page=0,
        repos=repos,
        cfg=cfg,
        i18n=i18n,
        locale=locale,
    )

    keyboard = None
    if has_more and cfg.features.top_unlimited:
        keyboard = _build_top_keyboard(0, has_more=True)
    await _reply_html(message, text, reply_markup=keyboard)


@router.callback_query(F.data.startswith(CALLBACK_PREFIX_TOP_PAGE))
async def cb_top_page(query: CallbackQuery, repos: Repositories, cfg: AppConfig, i18n: I18n) -> None:
    if query.from_user is None or query.data is None:
        return
    if not cfg.features.top_unlimited:
        await query.answer(i18n.t("errors.feature_disabled", normalize_locale(query.from_user.language_code)), show_alert=True)
        return

    locale = normalize_locale(query.from_user.language_code)
    page_str = query.data.removeprefix(CALLBACK_PREFIX_TOP_PAGE)
    try:
        page = int(page_str)
        if page < 0:
            raise ValueError
    except ValueError:
        await query.answer("Invalid page", show_alert=True)
        return

    chat_id = query.message.chat.id if query.message else None
    if chat_id is None:
        await query.answer("No chat", show_alert=True)
        return
    chat_kind = ChatIdKind.from_chat_id(chat_id)

    text, has_more = await _render_top(
        requester_uid=query.from_user.id,
        chat_kind=chat_kind,
        page=page,
        repos=repos,
        cfg=cfg,
        i18n=i18n,
        locale=locale,
    )
    await query.answer()
    await query.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=_build_top_keyboard(page, has_more))


@router.message(Command("dick_of_day", "dod"))
async def cmd_dod(message: Message, repos: Repositories, incrementor: Incrementor, cfg: AppConfig, i18n: I18n) -> None:
    if message.from_user is None:
        return
    locale = normalize_locale(message.from_user.language_code)
    chat = ChatIdPartiality.from_chat_id(message.chat.id)
    chat_kind = chat.kind(cfg.features.chats_merging)

    if cfg.features.dod_selection_mode == DickOfDaySelectionMode.WEIGHTS:
        winner = await repos.users.get_random_active_member_with_poor_in_priority(chat_kind)
    elif cfg.features.dod_selection_mode == DickOfDaySelectionMode.EXCLUSION and cfg.dod_rich_exclusion_ratio:
        winner = await repos.users.get_random_active_poor_member(chat_kind, cfg.dod_rich_exclusion_ratio)
    else:
        winner = await repos.users.get_random_active_member(chat_kind)

    if winner is None:
        await _reply_html(message, i18n.t("commands.dod.no_candidates", locale))
        return

    incr = await incrementor.dod_increment(message.from_user.id, chat_kind)
    try:
        result = await repos.dicks.set_dod_winner(chat, winner.uid, incr.total)
        if result is None:
            await _reply_html(message, i18n.t("commands.dod.no_candidates", locale))
            return
        answer = i18n.t(
            "commands.dod.result",
            locale,
            uid=winner.uid,
            name=escape_html(winner.name),
            growth=incr.total,
            length=result.new_length,
        )
        if result.pos_in_top is not None:
            answer = f"{answer}\n{i18n.t('commands.dod.position', locale, pos=result.pos_in_top)}"
    except asyncpg.PostgresError as e:
        if getattr(e, "sqlstate", None) == "GD0E2":
            answer = i18n.t("commands.dod.already_chosen", locale, name=str(getattr(e, "message", "")))
        else:
            raise

    answer = f"{answer}{time_till_next_day(i18n, locale)}"
    await _reply_html(message, answer)


@router.message(Command("loan", "borrow"))
async def cmd_loan(message: Message, repos: Repositories, cfg: AppConfig, i18n: I18n) -> None:
    if message.from_user is None:
        return
    locale = normalize_locale(message.from_user.language_code)
    chat_kind = ChatIdKind.from_chat_id(message.chat.id)

    maybe_loan = await repos.loans.get_active_loan(message.from_user.id, chat_kind)
    if maybe_loan and not cfg.features.multiple_loans:
        await _reply_html(message, i18n.t("commands.loan.debt", locale, debt=maybe_loan.debt))
        return

    if cfg.loan_payout_ratio <= 0.0 or cfg.loan_payout_ratio >= 1.0:
        await _reply_html(message, i18n.t("errors.feature_disabled", locale))
        return

    length = await repos.dicks.fetch_length(message.from_user.id, chat_kind)
    if length >= 0:
        await _reply_html(message, i18n.t("commands.loan.errors.positive_length", locale))
        return

    debt = abs(length)
    payout_percentage = f"{cfg.loan_payout_ratio * 100.0:.2f}%"

    data_confirm = f"loan:{message.from_user.id}:confirmed:{debt}:{cfg.loan_payout_ratio}"
    data_refuse = f"loan:{message.from_user.id}:refused"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t("commands.loan.confirmation.buttons.agree", locale),
                    callback_data=data_confirm,
                ),
                InlineKeyboardButton(
                    text=i18n.t("commands.loan.confirmation.buttons.disagree", locale),
                    callback_data=data_refuse,
                ),
            ]
        ]
    )
    await _reply_html(
        message,
        i18n.t("commands.loan.confirmation.text", locale, debt=debt, payout_percentage=payout_percentage),
        reply_markup=keyboard,
    )

@router.message(Command("stats"))
async def cmd_stats(message: Message, repos: Repositories, cfg: AppConfig, i18n: I18n) -> None:
    if message.from_user is None:
        return
    if not cfg.features.pvp.show_stats:
        return
    locale = normalize_locale(message.from_user.language_code)

    if message.chat.type == "private":
        stats = await repos.personal_stats.get(message.from_user.id)
        await _reply_html(
            message,
            i18n.t(
                "commands.stats.personal",
                locale,
                chats=stats.chats,
                max_length=stats.max_length,
                total_length=stats.total_length,
            ),
        )
        return

    chat_kind = ChatIdKind.from_chat_id(message.chat.id)
    dick = await repos.dicks.fetch_dick(message.from_user.id, chat_kind)
    length = dick.length if dick else 0
    pos = dick.position if dick and dick.position is not None else 0

    length_stats = i18n.t("commands.stats.length", locale, length=length, pos=pos)
    pvp = await repos.pvp_stats.get_stats(chat_kind, message.from_user.id)
    pvp_stats = i18n.t(
        "commands.stats.pvp",
        locale,
        win_rate=pvp.win_rate_formatted(),
        battles=pvp.battles_total,
        wins=pvp.battles_won,
        win_streak=pvp.win_streak_max,
        acquired=pvp.acquired_length,
        lost=pvp.lost_length,
    )
    if cfg.features.pvp.show_stats_notice:
        notice = i18n.t("commands.stats.notice", locale)
        pvp_stats = f"{pvp_stats}\n\n<i>{notice}</i>"
    await _reply_html(message, f"{length_stats}\n\n{pvp_stats}")


@router.message(Command("promo"))
async def cmd_promo(message: Message, command: CommandObject, state: FSMContext, repos: Repositories, i18n: I18n) -> None:  # type: ignore[no-untyped-def]
    if message.from_user is None:
        return
    if message.chat.type != "private":
        return
    locale = normalize_locale(message.from_user.language_code)
    code = (command.args or "").strip()
    if not code:
        await state.set_state(PromoFlow.requested)
        await _reply_html(message, i18n.t("commands.promo.request", locale))
        return
    await state.clear()
    await _activate_promo(message, code, repos, i18n, locale)


@router.message(PromoFlow.requested)
async def promo_requested(message: Message, state: FSMContext, repos: Repositories, i18n: I18n) -> None:
    if message.from_user is None:
        return
    locale = normalize_locale(message.from_user.language_code)
    if not message.text:
        await _reply_html(message, i18n.t("commands.promo.request", locale))
        return
    await state.clear()
    await _activate_promo(message, message.text.strip(), repos, i18n, locale)


async def _activate_promo(message: Message, code: str, repos: Repositories, i18n: I18n, locale: str) -> None:
    import re

    if not re.fullmatch(r"[a-zA-Z0-9_-]{4,16}", code or ""):
        await _reply_html(message, i18n.t("commands.promo.errors.no_activations_left", locale))
        return
    try:
        res = await repos.promo.activate(message.from_user.id, code)
        suffix = "plural" if res.chats_affected > 1 else "singular"
        ending = i18n.t(
            f"commands.promo.success.{suffix}",
            locale,
            growth=res.bonus_length,
            affected_chats=res.chats_affected,
            word_chats=_word_chats_ru(res.chats_affected),
        )
        await _reply_html(message, i18n.t("commands.promo.success.template", locale, ending=ending))
    except PromoActivationError as e:
        await _reply_html(message, i18n.t(f"commands.promo.errors.{e.code}", locale))


@router.callback_query(F.data.startswith("loan:"))
async def cb_loan(query: CallbackQuery, repos: Repositories, cfg: AppConfig, i18n: I18n) -> None:
    if query.data is None or query.from_user is None:
        return
    parts = query.data.split(":")
    if len(parts) < 3:
        await query.answer("Invalid data", show_alert=True)
        return
    _, uid_str, action = parts[:3]
    try:
        uid = int(uid_str)
    except ValueError:
        await query.answer("Invalid data", show_alert=True)
        return
    locale = normalize_locale(query.from_user.language_code)
    if uid != query.from_user.id:
        await query.answer("Not allowed", show_alert=True)
        return

    if action == "refused":
        await query.answer()
        if query.message:
            try:
                await query.message.delete()
            except Exception:
                await query.message.edit_text(i18n.t("commands.loan.callback.refused", locale), parse_mode=ParseMode.HTML)
        return

    if action != "confirmed":
        await query.answer("Invalid action", show_alert=True)
        return
    if len(parts) < 5:
        # old format without payout ratio -> treat as changed
        await query.answer()
        if query.message:
            await query.message.edit_text(i18n.t("commands.loan.callback.payout_ratio_changed", locale), parse_mode=ParseMode.HTML)
        return
    try:
        value = int(parts[3])
        payout_ratio = float(parts[4])
    except ValueError:
        await query.answer("Invalid data", show_alert=True)
        return

    if cfg.loan_payout_ratio <= 0.0:
        await query.answer(i18n.t("errors.feature_disabled", locale), show_alert=True)
        return
    if payout_ratio != cfg.loan_payout_ratio:
        await query.answer()
        if query.message:
            await query.message.edit_text(i18n.t("commands.loan.callback.payout_ratio_changed", locale), parse_mode=ParseMode.HTML)
        return

    if not query.message:
        await query.answer("No message", show_alert=True)
        return
    await repos.loans.borrow(uid, ChatIdKind.from_chat_id(query.message.chat.id), value)
    await query.answer()
    if query.message:
        await query.message.edit_text(i18n.t("commands.loan.callback.success", locale), parse_mode=ParseMode.HTML)


def _new_short_timestamp() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000) - TIMESTAMP_MILLIS_SINCE_2024


@router.message(Command("pvp", "battle", "attack", "fight"))
async def cmd_pvp(message: Message, command: CommandObject, repos: Repositories, cfg: AppConfig, i18n: I18n) -> None:  # type: ignore[no-untyped-def]
    if message.from_user is None:
        return
    locale = normalize_locale(message.from_user.language_code)
    if not command.args:
        await _reply_html(message, i18n.t("commands.pvp.errors.no_args", locale))
        return
    try:
        bet = int(command.args.strip())
        if bet <= 0:
            raise ValueError
    except ValueError:
        await _reply_html(message, i18n.t("commands.pvp.errors.no_args", locale))
        return

    chat = ChatIdPartiality.from_chat_id(message.chat.id)
    chat_kind = chat.kind(cfg.features.chats_merging)
    enough = await repos.dicks.check_dick(chat_kind, message.from_user.id, bet)
    if not enough:
        await _reply_html(message, i18n.t("commands.pvp.errors.not_enough.initiator", locale))
        return

    name = escape_html(get_full_name(message.from_user))
    text = i18n.t("commands.pvp.results.start", locale, name=name, bet=bet)
    btn_label = i18n.t("commands.pvp.button", locale)
    btn_data = f"{CALLBACK_PREFIX_PVP}{message.from_user.id}:{bet}:{_new_short_timestamp()}"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=btn_label, callback_data=btn_data)]]
    )
    await _reply_html(message, text, reply_markup=keyboard)


@router.callback_query(F.data.startswith(CALLBACK_PREFIX_PVP))
async def cb_pvp(query: CallbackQuery, repos: Repositories, cfg: AppConfig, i18n: I18n) -> None:
    if query.data is None or query.from_user is None:
        return
    locale = normalize_locale(query.from_user.language_code)
    parts = query.data.split(":")
    if len(parts) < 3:
        await query.answer(i18n.t("inline.callback.errors.invalid_data", locale), show_alert=True)
        return
    _, initiator_str, bet_str = parts[:3]
    try:
        initiator_uid = int(initiator_str)
        bet = int(bet_str)
    except ValueError:
        await query.answer(i18n.t("inline.callback.errors.invalid_data", locale), show_alert=True)
        return

    if initiator_uid == query.from_user.id:
        await query.answer(i18n.t("commands.pvp.errors.same_person", locale), show_alert=True)
        return

    if not query.message:
        await query.answer(i18n.t("inline.callback.errors.no_data", locale), show_alert=True)
        return
    chat = ChatIdPartiality.from_chat_id(query.message.chat.id)
    chat_kind = chat.kind(cfg.features.chats_merging)

    enough_initiator = await repos.dicks.check_dick(chat_kind, initiator_uid, bet)
    enough_acceptor = await repos.dicks.check_dick(
        chat_kind,
        query.from_user.id,
        bet if cfg.features.pvp.check_acceptor_length else 0,
    )

    if not enough_acceptor:
        await query.answer(i18n.t("commands.pvp.errors.not_enough.acceptor", locale), show_alert=True)
        return
    if not enough_initiator:
        await query.answer()
        await query.message.edit_text(i18n.t("commands.pvp.errors.not_enough.initiator", locale), parse_mode=ParseMode.HTML)
        return

    import secrets

    if secrets.SystemRandom().randbelow(2) == 0:
        winner_uid, loser_uid = initiator_uid, query.from_user.id
    else:
        winner_uid, loser_uid = query.from_user.id, initiator_uid

    loser_res, winner_res = await repos.dicks.move_length(chat, from_uid=loser_uid, to_uid=winner_uid, length=bet)

    withheld_part = ""
    loan = await repos.loans.get_active_loan(winner_uid, chat_kind)
    if loan is not None:
        payout = int(round(loan.payout_ratio * bet))
        payout = min(payout, loan.debt)
        if payout > 0:
            await repos.loans.pay(winner_uid, chat_kind, payout)
            winner_res = await repos.dicks.grow_no_attempts_check(chat_kind, winner_uid, -payout)
            withheld_part = "\n\n" + i18n.t("commands.pvp.results.withheld", locale, payout=payout)

    stats_part = ""
    if cfg.features.pvp.show_stats:
        winner_stats, loser_wr, loser_prev_streak = await repos.pvp_stats.send_battle_result(
            chat_kind, winner_uid, loser_uid, bet
        )
        stats_text = i18n.t(
            "commands.pvp.results.stats.text",
            locale,
            winner_win_rate=winner_stats.win_rate_formatted(),
            loser_win_rate=f"{loser_wr:.2f}%",
            winner_win_streak=winner_stats.win_streak_current,
            winner_win_streak_max=winner_stats.win_streak_max,
        )
        if loser_prev_streak > 1:
            stats_text += "\n" + i18n.t("commands.pvp.results.stats.lost_win_streak", locale, lost_win_streak=loser_prev_streak)
        stats_part = "\n\n" + stats_text

    winner_user = await repos.users.get(winner_uid)
    loser_user = await repos.users.get(loser_uid)
    winner_name = escape_html(winner_user.name if winner_user else str(winner_uid))
    loser_name = escape_html(loser_user.name if loser_user else str(loser_uid))

    main_part = i18n.t(
        "commands.pvp.results.finish",
        locale,
        winner_name=winner_name,
        winner_length=winner_res.new_length,
        loser_length=loser_res.new_length,
        bet=bet,
    )

    if winner_res.pos_in_top is not None and loser_res.pos_in_top is not None:
        winner_pos = i18n.t("commands.pvp.results.position.winner", locale, name=winner_name, pos=winner_res.pos_in_top)
        loser_pos = i18n.t("commands.pvp.results.position.loser", locale, name=loser_name, pos=loser_res.pos_in_top)
        main_part = f"{main_part}\n\n{winner_pos}\n{loser_pos}"

    await query.answer()
    await query.message.edit_text(f"{main_part}{withheld_part}{stats_part}", parse_mode=ParseMode.HTML)


@router.message(Command("import"))
async def cmd_import(message: Message, bot: Bot, repos: Repositories, i18n: I18n) -> None:
    if message.from_user is None:
        return
    locale = normalize_locale(message.from_user.language_code)
    if message.chat.type == "private":
        await _reply_html(message, i18n.t("errors.not_group_chat", locale))
        return

    # Admin-only
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    if member.status not in ("administrator", "creator"):
        await _reply_html(message, i18n.t("commands.import.errors.not_admin", locale))
        return

    reply = message.reply_to_message
    if reply is None or getattr(reply, "forward_origin", None) is not None:
        origin_bots = ", ".join(f"@{b}" for b in sorted(ORIGINAL_BOT_USERNAMES))
        await _reply_html(message, i18n.t("commands.import.errors.not_reply", locale, origin_bots=origin_bots))
        return
    if reply.from_user is None or not reply.from_user.is_bot or not reply.from_user.username:
        origin_bots = ", ".join(f"@{b}" for b in sorted(ORIGINAL_BOT_USERNAMES))
        await _reply_html(message, i18n.t("commands.import.errors.not_reply", locale, origin_bots=origin_bots))
        return
    original_bot = reply.from_user.username
    if original_bot not in ORIGINAL_BOT_USERNAMES:
        origin_bots = ", ".join(f"@{b}" for b in sorted(ORIGINAL_BOT_USERNAMES))
        await _reply_html(message, i18n.t("commands.import.errors.not_reply", locale, origin_bots=origin_bots))
        return
    text = reply.text or ""
    if not text:
        origin_bots = ", ".join(f"@{b}" for b in sorted(ORIGINAL_BOT_USERNAMES))
        await _reply_html(message, i18n.t("commands.import.errors.not_reply", locale, origin_bots=origin_bots))
        return

    import re

    top_line_re = re.compile(r"\d{1,3}((\\. )|\\|)(?P<name>.+?)(\\.{3})? — (?P<length>\\d+) см\\.")

    lines = text.splitlines()
    # skip header lines until first match
    while lines and not top_line_re.search(lines[0]):
        lines = lines[1:]
    matches = []
    invalid = []
    for line in lines:
        m = top_line_re.search(line)
        if not m:
            invalid.append(line)
        else:
            matches.append(m)
    if invalid:
        invalid_lines = "\n".join(i18n.t("commands.import.errors.invalid_lines.line", locale, line=escape_html(l)) for l in invalid)
        await _reply_html(message, i18n.t("commands.import.errors.invalid_lines.template", locale, invalid_lines=invalid_lines))
        return

    chat_kind = ChatIdKind.from_chat_id(message.chat.id)
    members = await repos.users.get_chat_members(chat_kind)
    members_by_short = {
        _convert_name_for_import(original_bot, m.name): (m.uid, m.name)
        for m in members
    }
    member_names = set(members_by_short.keys())

    parsed = []
    for m in matches:
        name = (m.group("name") or "").strip()
        length = int(m.group("length"))
        parsed.append((name, length))

    existing = [(n, l) for (n, l) in parsed if n in member_names]
    not_found = [n for (n, _) in parsed if n not in member_names]

    imported_uids = {u.uid for u in await repos.import_repo.get_imported_users(message.chat.id)}
    already_present = []
    to_import = []
    for short_name, length in existing:
        uid, full_name = members_by_short[short_name]
        if uid in imported_uids:
            already_present.append((full_name, length))
        else:
            to_import.append((uid, full_name, length))

    await repos.import_repo.import_users(
        message.chat.id,
        [ExternalUser(uid=uid, length=length) for (uid, _, length) in to_import],
    )

    parts = []
    if to_import:
        title = i18n.t("commands.import.result.titles.imported", locale)
        lines = "\n".join(
            i18n.t("commands.import.result.line.imported", locale, name=escape_html(full_name), length=length)
            for (_, full_name, length) in to_import
        )
        parts.append(f"{title}\n{lines}")
    if already_present:
        title = i18n.t("commands.import.result.titles.already_present", locale)
        lines = "\n".join(
            i18n.t("commands.import.result.line.already_present", locale, name=escape_html(full_name), length=length)
            for (full_name, length) in already_present
        )
        parts.append(f"{title}\n{lines}")
    if not_found:
        title = i18n.t("commands.import.result.titles.not_found", locale)
        lines = "\n".join(
            i18n.t("commands.import.result.line.not_found", locale, name=escape_html(n))
            for n in not_found
        )
        parts.append(f"{title}\n{lines}")

    await _reply_html(message, "\n\n".join(parts) if parts else i18n.t("commands.import.result.titles.not_found", locale))

async def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    load_dotenv(repo_root / ".env", override=False)
    load_dotenv(override=False)

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    token = _require_env("TELOXIDE_TOKEN")
    cfg = load_config()

    db = await Database.connect()
    await apply_sql_migrations(db.pool, repo_root / "migrations")

    i18n = I18n.from_locales_dir(repo_root / "locales", fallback_locale="en")
    privacy_policy = load_privacy_policy(rust_privacy_dir=repo_root / "src" / "handlers" / "privacy")

    help_context = {
        "bot_name": "DickGrowerBot",
        "grow_min": str(int(os.getenv("GROWTH_MIN", "-5"))),
        "grow_max": str(int(os.getenv("GROWTH_MAX", "10"))),
        "other_bots": "@pipisabot, @kraft28_bot",
        "admin_channel_ru": "@" + _require_env("HELP_ADMIN_CHANNEL_RU").lstrip("@"),
        "admin_channel_en": "@" + _require_env("HELP_ADMIN_CHANNEL_EN").lstrip("@"),
        "admin_chat_ru": "@" + _require_env("HELP_ADMIN_CHAT_RU").lstrip("@"),
        "admin_chat_en": "@" + _require_env("HELP_ADMIN_CHAT_EN").lstrip("@"),
        "git_repo": _require_env("HELP_GIT_REPO"),
        "help_pussies_percentage": float(os.getenv("HELP_PUSSIES_COEF", "0.0")) * 100.0,
    }
    help_container = render_help_messages(rust_help_dir=repo_root / "src" / "help", context=help_context)

    repos = Repositories.create(db.pool, cfg)
    perks = [
        HelpPussiesPerk(coefficient=float(os.getenv("HELP_PUSSIES_COEF", "0.0"))),
        LoanPayoutPerk(loans=repos.loans),
    ]
    incrementor = Incrementor(repos.dicks, perks)

    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp["repos"] = repos
    dp["cfg"] = cfg
    dp["i18n"] = i18n
    dp["help_container"] = help_container
    dp["privacy_policy"] = privacy_policy
    dp["incrementor"] = incrementor

    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
