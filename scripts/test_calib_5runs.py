import asyncio
import ctypes
import statistics
import sys

sys.path.insert(0, ".")

if sys.platform == "win32":
    ctypes.windll.winmm.timeBeginPeriod(1)

from scripts.asr_latency_bench import run_instrument_calibration


async def main():
    errors = []
    print("Running 5 sequential calibration passes (delay=200.0ms):")
    for i in range(5):
        res = await run_instrument_calibration(delay_ms=200.0)
        err = res["error_ms"]
        errors.append(err)
        print(f"  Run {i + 1}: Measured={res['measured_ms']:.2f}ms | Error={err:.2f}ms")
    mean_err = statistics.mean(errors)
    max_err = max(errors)
    print("\nCalibration Results across 5 runs:")
    print(f"  Mean Error: {mean_err:.2f}ms")
    print(f"  Max Error:  {max_err:.2f}ms")
    print(f"  Contract:   <= 10.0ms -> [{'PASS' if max_err <= 10.0 else 'UNMET'}]")


if __name__ == "__main__":
    asyncio.run(main())
