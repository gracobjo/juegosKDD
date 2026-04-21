"""Utilidad compartida para invocar Claude API con degradacion elegante."""
from __future__ import annotations

import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

_CLIENT = None  # lazy
_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")


def _get_client():
    """Devuelve un cliente Anthropic si hay key valida, si no None."""
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not key or "TU_" in key or "YOUR_" in key:
        return None
    try:
        import anthropic
        _CLIENT = anthropic.Anthropic(api_key=key)
        return _CLIENT
    except Exception as exc:
        log.warning("No se pudo inicializar anthropic client: %s", exc)
        return None


def llm_available() -> bool:
    return _get_client() is not None


def call_claude(prompt: str, max_tokens: int = 400, system: Optional[str] = None) -> str:
    """Llama a Claude. Si no esta disponible devuelve cadena vacia."""
    client = _get_client()
    if client is None:
        return ""
    try:
        kwargs = {
            "model": _MODEL,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system
        response = client.messages.create(**kwargs)
        parts = []
        for block in response.content:
            text = getattr(block, "text", None)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    except Exception as exc:
        log.warning("call_claude fallo: %s", exc)
        return ""
