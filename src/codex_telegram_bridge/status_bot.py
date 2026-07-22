from __future__ import annotations

from telegram import Update
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    ContextTypes,
    TypeHandler,
)

from .discussion_bot import DiscussionBotController
from .store import Store
from .telegram_common import STATUS_ROLE, TelegramEndpoint

_STATUS_ACTIONS = frozenset({"space_refresh", "space_unwatch"})


class StatusBotController:
    """Callback-only controller for status messages owned by the 69 Bot."""

    def __init__(
        self,
        store: Store,
        discussion_controller: DiscussionBotController,
        endpoint: TelegramEndpoint,
    ) -> None:
        self.store = store
        self.discussion_controller = discussion_controller
        self.endpoint = endpoint

    def install(self, application: Application) -> None:
        application.add_handler(TypeHandler(Update, self._guard), group=-100)
        application.add_handler(CallbackQueryHandler(self.callback, pattern=r"^cb:"))

    async def set_commands(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def _guard(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        query = update.callback_query
        message = getattr(query, "message", None) if query else None
        chat = getattr(message, "chat", None)
        user = update.effective_user
        if not query or not chat or chat.type != ChatType.SUPERGROUP or not user:
            raise ApplicationHandlerStop
        if not self.store.claim_telegram_update(update.update_id, bot_role=STATUS_ROLE):
            raise ApplicationHandlerStop
        binding = self.store.get_telegram_binding()
        owner = self.store.get_owner()
        if (
            not binding
            or int(chat.id) != int(binding["discussion_chat_id"])
            or owner is None
            or int(user.id) != int(owner.user_id)
        ):
            raise ApplicationHandlerStop

    async def callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self.discussion_controller.callback_for_role(
            update,
            context,
            bot_role=STATUS_ROLE,
            endpoint=self.endpoint,
            allowed_actions=_STATUS_ACTIONS,
        )
