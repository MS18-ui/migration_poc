"""Azure OpenAI provider with function (tool) calling.

Env vars:
  AZURE_OPENAI_ENDPOINT      e.g. https://my-resource.openai.azure.com/   (required)
  AZURE_OPENAI_DEPLOYMENT    your *deployment name*, not the model name   (required)
  AZURE_OPENAI_API_KEY       key auth; if unset, Microsoft Entra ID is used
                             (DefaultAzureCredential; needs `pip install azure-identity`)
  AZURE_OPENAI_API_VERSION   default 2024-10-21
"""
import json
import os
from typing import List

from openai import AzureOpenAI

from .config import azure_settings
from .llm import BaseLLM, LLMResponse, ToolCall, ToolResult

DEFAULT_API_VERSION = "2024-10-21"


class AzureOpenAILLM(BaseLLM):
    def __init__(self, deployment: str = None, endpoint: str = None, api_key: str = None,
                 api_version: str = None, max_tokens: int = 4096):
        cfg = azure_settings()  # env vars > config.yaml
        endpoint = endpoint or cfg["endpoint"]
        self.deployment = deployment or cfg["deployment"]
        api_key = api_key or cfg["api_key"]
        api_version = api_version or cfg["api_version"] or DEFAULT_API_VERSION
        max_tokens = cfg["max_tokens"] if max_tokens == 4096 else max_tokens
        if not endpoint or not self.deployment:
            raise EnvironmentError(
                "Set endpoint and deployment in config.yaml (azure_openai) or via "
                "AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_DEPLOYMENT.")
        # Accept the v1-style URL (https://<res>.openai.azure.com/openai/v1) by reducing it
        # to the resource root, which AzureOpenAI expects.
        endpoint = endpoint.rstrip("/")
        for suffix in ("/openai/v1", "/openai"):
            if endpoint.endswith(suffix):
                endpoint = endpoint[: -len(suffix)]
                break

        kwargs = dict(azure_endpoint=endpoint, api_version=api_version)
        if api_key:
            kwargs["api_key"] = api_key
        else:  # keyless: Microsoft Entra ID
            try:
                from azure.identity import DefaultAzureCredential, get_bearer_token_provider
            except ImportError as e:
                raise EnvironmentError(
                    "No AZURE_OPENAI_API_KEY set; install azure-identity for Entra ID auth."
                ) from e
            kwargs["azure_ad_token_provider"] = get_bearer_token_provider(
                DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
            )
        self.client = AzureOpenAI(**kwargs)
        self.max_tokens = max_tokens

    # -- adapters ---------------------------------------------------------------------
    @staticmethod
    def _to_openai_tools(tools: List[dict]) -> List[dict]:
        return [{"type": "function",
                 "function": {"name": t["name"], "description": t["description"],
                              "parameters": t["input_schema"]}} for t in tools]

    def chat(self, system, history, tools) -> LLMResponse:
        resp = self.client.chat.completions.create(
            model=self.deployment,  # Azure: deployment name
            messages=[{"role": "system", "content": system}] + history,
            tools=self._to_openai_tools(tools),
            tool_choice="auto",
            max_completion_tokens=self.max_tokens,
        )
        msg = resp.choices[0].message
        calls = []
        for tc in msg.tool_calls or []:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}  # tool will return an ERROR for missing 'content'; model retries
            calls.append(ToolCall(tc.id, tc.function.name, args))
        return LLMResponse(text=msg.content or "", tool_calls=calls, raw=msg)

    def user_message(self, text):
        return {"role": "user", "content": text}

    def assistant_message(self, resp: LLMResponse):
        m = {"role": "assistant", "content": resp.text or None}
        if resp.tool_calls:
            m["tool_calls"] = [
                {"id": c.id, "type": "function",
                 "function": {"name": c.name, "arguments": json.dumps(c.args)}}
                for c in resp.tool_calls
            ]
        return m

    def tool_result_messages(self, results: List[ToolResult]):
        # One `tool` message per call (errors already prefixed "ERROR:" by the registry)
        return [{"role": "tool", "tool_call_id": r.id, "content": r.output} for r in results]
