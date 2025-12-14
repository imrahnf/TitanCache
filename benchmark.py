import aiohttp
import asyncio
import time
import random
import csv
import sys
# import numpy as np # Uncomment if numpy is installed

# --- CONFIGURATION ---
BASE_URL = "http://localhost:8080/api/cache"
CSV_FILENAME = "titan_nuclear_benchmark.csv"

# --- SCENARIO DEFINITIONS ---
SCENARIOS = [
    {
        "name": "1_Realistic_E_Commerce_Day",
        "requests": 100000,
        "users": 1000,
        "read_ratio": 0.9,
        "key_pattern": "zipf",
        "payload_mean": 5120,
        "payload_std": 1024,
        "key_space": 50000
    },
    {
        "name": "2_Flash_Sale_Spike",
        "requests": 50000,
        "users": 3000,
        "read_ratio": 0.2,
        "key_pattern": "hot",
        "payload_mean": 1024,
        "payload_std": 100,
        "key_space": 1000
    },
    {
        "name": "3_The_Ram_Eater",
        "requests": 50000,
        "users": 200,
        "read_ratio": 0.05,
        "key_pattern": "uniform",
        "payload_mean": 50 * 1024,
        "payload_std": 0,
        "key_space": 200000
    }
]

# --- UTILITIES ---

async def clear_server_state(session):
    """Resets the server cache state before a new scenario."""
    try:
        async with session.delete(f"{BASE_URL}/clear") as resp:
            print(f"[INFO] Cache Reset Status: {resp.status}")
    except Exception as e:
        print(f"[WARN] Reset Failed: {e}")

def generate_payload(mean, std):
    """Generates a dummy payload string with Gaussian distributed size."""
    size = int(random.gauss(mean, std))
    return "X" * max(1, size)

def get_key(config):
    """Selects a key based on the configured access pattern."""
    pattern = config["key_pattern"]
    space = config["key_space"]

    if pattern == "uniform":
        return f"user_{random.randint(1, space)}"

    elif pattern == "hot":
        if random.random() < 0.9:
            return f"user_{random.randint(1, int(space * 0.05))}"
        return f"user_{random.randint(1, space)}"

    elif pattern == "zipf":
        k = int(space * (random.random() ** 3))
        return f"user_{max(1, k)}"

async def worker(session, i, config, stats_list, error_counter):
    """Performs a single request and records metrics."""
    key = get_key(config)
    value = generate_payload(config["payload_mean"], config["payload_std"])
    op_type = "READ"
    start = time.time()
    status = "ERR"

    try:
        if random.random() > config["read_ratio"]:
            op_type = "WRITE"
            async with session.post(f"{BASE_URL}/store", json={"key": key, "value": value}) as response:
                await response.read()
                status = response.status
        else:
            async with session.get(f"{BASE_URL}/retrieve/{key}") as response:
                await response.read()
                status = response.status

        latency_ms = (time.time() - start) * 1000

        stats_list.append([
            config["name"],
            time.time(),
            op_type,
            status,
            round(latency_ms, 2),
            len(value),
            key
        ])

    except Exception as e:
        # Increment shared error counter
        error_counter['count'] += 1
        pass

async def run_scenario(scenario):
    print(f"\n[STARTING SCENARIO] {scenario['name']}")
    print(f" > Configuration: {scenario['requests']} requests | {scenario['users']} concurrent users")
    print(f" > Payload Avg:   {scenario['payload_mean']/1024:.2f} KB")

    stats_list = []
    error_counter = {'count': 0}

    # Increase TCP limit and TIMEOUT (300s = 5 mins) to prevent "Ram Eater" timeouts
    connector = aiohttp.TCPConnector(limit=scenario["users"] + 100, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=300)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        await clear_server_state(session)
        await asyncio.sleep(2)

        start_time = time.time()
        tasks = []

        total_reqs = scenario["requests"]

        # Batch execution
        for i in range(total_reqs):
            tasks.append(worker(session, i, scenario, stats_list, error_counter))

            # Execute batch
            if len(tasks) >= scenario["users"]:
                await asyncio.gather(*tasks)
                tasks = []

                # --- PROGRESS LOGGING ---
                # Print progress every 10%
                if (i + 1) % (total_reqs // 10) == 0:
                    percent = ((i + 1) / total_reqs) * 100
                    elapsed = time.time() - start_time
                    print(f"   ... {percent:.0f}% complete ({elapsed:.1f}s elapsed)")

        # Process remaining tasks
        if tasks:
            await asyncio.gather(*tasks)

        total_time = time.time() - start_time
        throughput = scenario["requests"] / total_time
        data_volume_mb = (scenario['requests'] * scenario['payload_mean']) / (1024 * 1024)

        print(f"[COMPLETE] Execution Time: {total_time:.2f}s")
        print(f" > Throughput: {throughput:.0f} req/sec")
        print(f" > Data Moved: {data_volume_mb:.2f} MB")
        if error_counter['count'] > 0:
            print(f" > [WARN] Errors/Timeouts encountered: {error_counter['count']}")

    return stats_list

async def main():
    print("--------------------------------------------------")
    print(" TITAN BENCHMARK SUITE - INITIALIZED")
    print("--------------------------------------------------")

    all_results = []

    for scenario in SCENARIOS:
        results = await run_scenario(scenario)
        all_results.extend(results)
        await asyncio.sleep(5)

    print("\n--------------------------------------------------")
    print(f"[EXPORTING] Writing {len(all_results)} records to {CSV_FILENAME}...")

    with open(CSV_FILENAME, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Scenario", "Timestamp", "Operation", "HTTP_Status", "Latency_ms", "Payload_Bytes", "Key_ID"])
        writer.writerows(all_results)

    print("[SUCCESS] Benchmark suite completed successfully.")
    print("--------------------------------------------------")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())