import os

from mistralai import Mistral


api_key = '0TD9nsBifR6Lkr1kOag9aikbCBImYfGg'#os.getenv("MISTRAL_API_KEY")
model = os.getenv("MISTRAL_MODEL", "mistral-medium-latest")

if not api_key:
    raise SystemExit("Set MISTRAL_API_KEY before running this test.")

client = Mistral(api_key=api_key)
chat_response = client.chat.complete(
    model=model,
    messages=[{"role": "user", "content": "What is the best French cheese?"}],
)
print(chat_response.choices[0].message.content)
