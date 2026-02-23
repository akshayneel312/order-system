import subprocess
import time

#config
TOPIC = "orders_topic"
GROUP = "order_group"
KAFKA_CONTAINER = "order_system-kafka-1"
MAX_CONSUMERS = 10
THRESHOLD = 10

current_consumers = 1

def get_kafka_lag():
    try:
        cmd = [
            "docker", "exec", KAFKA_CONTAINER,
            "kafka-consumer-groups",
            "--bootstrap-server", "localhost:9092",
            "--describe",
            "--group", GROUP
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        total_lag = 0
        lines = result.stdout.strip().split('\n')
        
        #sum up the 'LAG' column
        for line in lines[1:]:
            parts = line.split()
            #data row matching our group
            if len(parts) > 5 and parts[0] == GROUP:
                lag_str = parts[5]
                if lag_str.isdigit():
                    total_lag += int(lag_str)
                    
        return total_lag
    except Exception as e:
        print(f"Error checking lag: {e}")
        return 0

def scale_consumers(target_count):
    #spin up or tear down Web containers
    print(f"Scaling consumers to {target_count}...")
    subprocess.run(["docker", "compose", "up", "-d", "--scale", f"web={target_count}"])

print("Autoscaler for Docker Compose")

while True:
    lag = get_kafka_lag()
    print(f"Current Kafka Lag: {lag} | Active Consumers: {current_consumers}")
    
    #scale up logic
    if lag > THRESHOLD and current_consumers < MAX_CONSUMERS:
        current_consumers += 1
        print(f"Lag is over {THRESHOLD} Adding a new consumer")
        scale_consumers(current_consumers)
        time.sleep(10)
        
    #scale down logic
    elif lag == 0 and current_consumers > 1:
        current_consumers -= 1
        print(f"Queue is empty. Removing a consumer to save resources")
        scale_consumers(current_consumers)
        time.sleep(10)
        
    #check every 3 sec
    time.sleep(3)