from __future__ import annotations

from dataclasses import dataclass

from .incrementor import ChangeIntent, Perk
from .repo import ChatIdKind, LoansRepo


@dataclass(frozen=True)
class HelpPussiesPerk(Perk):
    name: str = "help-pussies"
    coefficient: float = 0.0

    def enabled(self) -> bool:
        return self.coefficient > 0.0

    async def apply(self, uid: int, chat: ChatIdKind, intent: ChangeIntent) -> int:
        if intent.current_length >= 0:
            return 0
        current_deepness = abs(intent.current_length)
        return int(round(self.coefficient * current_deepness))


@dataclass(frozen=True)
class LoanPayoutPerk(Perk):
    loans: LoansRepo
    name: str = "loan-payout"

    async def apply(self, uid: int, chat: ChatIdKind, intent: ChangeIntent) -> int:
        loan = await self.loans.get_active_loan(uid, chat)
        if loan is None:
            return 0
        if intent.base_increment <= 0:
            return 0
        payout = int(round(intent.base_increment * loan.payout_ratio))
        if payout <= 0:
            return 0
        payout = min(payout, loan.debt)
        await self.loans.pay(uid, chat, payout)
        return -payout

