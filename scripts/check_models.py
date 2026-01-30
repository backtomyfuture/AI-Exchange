
import os
import sys
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_llm():
    print("\n--- Testing LLM (Reasoning) ---")
    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_API_BASE")
    model = os.getenv("LLM_MODEL", "gemini-3-flash")

    print(f"Endpoint: {base_url}")
    print(f"Model: {model}")

    if not base_url:
        print("Error: OPENAI_API_BASE not set.")
        return False

    try:
        client = OpenAI(base_url=base_url, api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Hello, are you working?"}],
            max_tokens=50
        )
        msg = response.choices[0].message
        content = msg.content
        if content:
            print(content.strip())
        else:
            print(f"(No content returned). Message object: {msg}")
        return True
    except Exception as e:
        print(f"Failed to connect to LLM: {e}")
        return False

def check_embeddings():
    print("\n--- Testing Embeddings ---")
    
    base_url = os.getenv("EMBEDDING_BASE_URL") or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
    api_key = os.getenv("EMBEDDING_API_KEY", "ollama")
    model = os.getenv("EMBEDDING_MODEL", "qwen3-embedding:4b")

    print(f"Endpoint: {base_url}")
    print(f"Model: {model}")

    try:
        client = OpenAI(base_url=base_url, api_key=api_key)
        response = client.embeddings.create(
            input="Test embedding string",
            model=model
        )
        embedding = response.data[0].embedding
        print(f"Success! Generated embedding with dimension: {len(embedding)}")
        return True
    except Exception as e:
        print(f"Failed to connect to Embedding Service: {e}")
        return False

if __name__ == "__main__":
    llm_ok = check_llm()
    emb_ok = check_embeddings()

    if llm_ok and emb_ok:
        print("\n✅ All systems operational.")
        sys.exit(0)
    else:
        print("\n❌ One or more systems failed.")
        sys.exit(1)
