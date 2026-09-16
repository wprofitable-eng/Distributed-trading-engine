from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict

import requests
from requests import Session
from requests.exceptions import RequestException, Timeout


logger = logging.getLogger(__name__)


class TelegramPollingBot:
    def __init__(self, token: str, chat_id: str, command_handler: Callable[[str], str]) -> None:
        self.token = token
        self.chat_id = chat_id
        self.command_handler = command_handler
        self.running = False
        self.offset = 0
        self.base = f"https://api.telegram.org/bot{token}"
        self.session: Session = requests.Session()

    def start(self) -> None:
        if not self.token:
            return
        self.running = True
        threading.Thread(target=self._poll, daemon=True).start()

    def _send(self, text: str, chat_id: str | None = None) -> None:
        try:
            self.session.post(
                f"{self.base}/sendMessage",
                json={"chat_id": chat_id or self.chat_id, "text": text},
                timeout=8,
            )
        except Timeout:
            logger.warning("Telegram send timed out")
        except RequestException as exc:
            logger.warning("Telegram send failed: %s", exc)

    def _poll(self) -> None:
        consecutive_failures = 0
        while self.running:
            started = time.time()
            try:
                r = self.session.get(
                    f"{self.base}/getUpdates",
                    params={"timeout": 8, "offset": self.offset},
                    timeout=12,
                )
                data: Dict = r.json()
                consecutive_failures = 0
                for item in data.get("result", []):
                    self.offset = item["update_id"] + 1
                    message = item.get("message", {})
                    incoming_chat_id = str(message.get("chat", {}).get("id", "")).strip()
                    if self.chat_id and incoming_chat_id and incoming_chat_id != str(self.chat_id).strip():
                        continue
                    text = message.get("text", "").strip()
                    if text in {"/start", "/status", "/pnl", "/autofutures", "/stopfutures", "/account"}:
                        reply = self.command_handler(text)
                        self._send(reply, chat_id=incoming_chat_id or self.chat_id)
            except Timeout:
                consecutive_failures += 1
                logger.warning("Telegram polling timed out (%s consecutive)", consecutive_failures)
            except RequestException as exc:
                consecutive_failures += 1
                logger.warning("Telegram polling failed (%s consecutive): %s", consecutive_failures, exc)
            elapsed = time.time() - started
            cooldown = min(8.0, float(max(0, consecutive_failures - 1)))
            if elapsed + cooldown < 1:
                time.sleep(1 - (elapsed + cooldown))
            elif cooldown > 0:
                time.sleep(cooldown)
