"""Provider-agnostic LLM layer. Agents talk only to BaseLLM; each provider adapts
its own wire format (messages, tool schema, tool results) behind it."""
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List


DEFAULT_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")


@dataclass
class ToolCall:
    id: str
    name: str
    args: dict


@dataclass
class ToolResult:
    id: str
    output: str
    is_error: bool = False


@dataclass
class LLMResponse:
    text: str = ""
    tool_calls: List[ToolCall] = field(default_factory=list)
    raw: Any = None  # provider-native response, used to rebuild assistant history


class BaseLLM(ABC):
    """Contract every provider implements. `tools` are neutral dicts:
    {"name", "description", "input_schema"}."""

    @abstractmethod
    def chat(self, system: str, history: list, tools: List[dict]) -> LLMResponse: ...

    @abstractmethod
    def user_message(self, text: str) -> dict: ...

    @abstractmethod
    def assistant_message(self, resp: LLMResponse) -> dict: ...

    @abstractmethod
    def tool_result_messages(self, results: List[ToolResult]) -> List[dict]: ...


class AnthropicLLM(BaseLLM):
    def __init__(self, model: str = None, max_tokens: int = None):
        from .config import anthropic_settings
        cfg = anthropic_settings()
        if not cfg["api_key"]:
            raise EnvironmentError("Set ANTHROPIC_API_KEY or anthropic.api_key in config.yaml.")
        from anthropic import Anthropic  # lazy: not needed for Azure-only use
        kwargs = {"api_key": cfg["api_key"]}
        if cfg["base_url"]:  # Azure AI Foundry: https://<res>.services.ai.azure.com/anthropic
            kwargs["base_url"] = cfg["base_url"]
        self.client = Anthropic(**kwargs)
        self.model = model or cfg["model"] or DEFAULT_MODEL
        self.max_tokens = max_tokens or cfg["max_tokens"]

    def chat(self, system, history, tools):
        resp = self.client.messages.create(
            model=self.model, max_tokens=self.max_tokens,
            system=system, messages=history, tools=tools,
        )
        calls = [ToolCall(b.id, b.name, b.input) for b in resp.content if b.type == "tool_use"]
        text = "".join(b.text for b in resp.content if b.type == "text")
        return LLMResponse(text=text, tool_calls=calls, raw=resp)

    def user_message(self, text):
        return {"role": "user", "content": text}

    def assistant_message(self, resp):
        return {"role": "assistant", "content": resp.raw.content}

    def tool_result_messages(self, results):
        return [{"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": r.id, "content": r.output, "is_error": r.is_error}
            for r in results
        ]}]


def create_llm(provider: str = None) -> BaseLLM:
    """provider: 'anthropic' | 'azure' (default from LLM_PROVIDER env, else anthropic)."""
    from .config import load_config
    provider = (provider or os.getenv("LLM_PROVIDER") or load_config().get("provider") or "anthropic").lower()
    if provider == "anthropic":
        return AnthropicLLM()
    if provider in ("azure", "azure_openai"):
        from .azure_openai import AzureOpenAILLM  # lazy: only needs `openai` when used
        return AzureOpenAILLM()
    raise ValueError(f"Unknown LLM provider '{provider}'. Use 'anthropic' or 'azure'.")
