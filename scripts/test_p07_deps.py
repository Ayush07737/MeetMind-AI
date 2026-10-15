"""P0-7: Test Component #6 dependency compatibility on Python 3.13 (Windows)."""

import sys

print(f"Testing on Python: {sys.version}")

results = {}

# 1. torch
try:
    import torch

    results["torch"] = f"OK (v{torch.__version__}, cuda_available={torch.cuda.is_available()})"
except Exception as e:
    results["torch"] = f"FAIL: {type(e).__name__}: {e}"

# 2. transformers
try:
    import transformers

    results["transformers"] = f"OK (v{transformers.__version__})"
except Exception as e:
    results["transformers"] = f"FAIL: {type(e).__name__}: {e}"

# 3. onnxruntime
try:
    import onnxruntime

    results["onnxruntime"] = f"OK (v{onnxruntime.__version__})"
except Exception as e:
    results["onnxruntime"] = f"FAIL: {type(e).__name__}: {e}"

# 4. optimum
try:
    import optimum
    import optimum.version

    results["optimum"] = f"OK (v{optimum.version.__version__})"
except Exception as e:
    results["optimum"] = f"FAIL: {type(e).__name__}: {e}"

# 5. anthropic
try:
    import anthropic

    results["anthropic"] = f"OK (v{anthropic.__version__})"
except Exception as e:
    results["anthropic"] = f"FAIL: {type(e).__name__}: {e}"

# 6. spacy
try:
    import spacy

    results["spacy"] = f"OK (v{spacy.__version__})"
except Exception as e:
    results["spacy"] = f"FAIL: {type(e).__name__}: {e}"

print("\n--- DEPENDENCY IMPORT RESULTS ---")
for pkg, status in results.items():
    print(f"  {pkg:15}: {status}")

# 7. spacy model en_core_web_sm
print("\n--- SPACY MODEL TEST ---")
try:
    import spacy

    try:
        nlp = spacy.load("en_core_web_sm")
        print("  en_core_web_sm: OK (pre-installed)")
    except OSError:
        print("  en_core_web_sm: not installed, downloading...")
        import subprocess

        subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])
        nlp = spacy.load("en_core_web_sm")
        doc = nlp("MeetMind AI processes real-time meeting transcripts.")
        print(f"  en_core_web_sm: OK (downloaded & loaded, tokens={len(doc)})")
except Exception as e:
    print(f"  en_core_web_sm: FAIL: {type(e).__name__}: {e}")
