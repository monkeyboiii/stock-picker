import random
import math

def simulate_poisson_events(probability, duration=30):
    """
    Simulates events in a Poisson process with a specified probability of at least one event occurring within the duration.
    
    Parameters:
    probability (float): The probability (0 < x < 1) of at least one event occurring in the given duration.
    duration (int): The total time period in minutes (default is 30 minutes).
    
    Returns:
    list: A sorted list of event times (in minutes) within the duration.
    """
    if probability <= 0 or probability >= 1:
        raise ValueError("Probability must be between 0 and 1.")
    
    # Calculate total lambda to achieve the desired probability of at least one event
    lambda_total = -math.log(1 - probability)
    
    # Rate parameter for the Poisson process (per minute)
    rate = lambda_total / duration
    
    events = []
    current_time = 0.0
    
    # Generate events using exponential inter-arrival times
    while True:
        inter_arrival = random.expovariate(rate)
        current_time += inter_arrival
        if current_time > duration:
            break
        events.append(current_time)
    
    return events

# Example usage:
event_probability = 0.7  # 70% chance of at least one event in 30 minutes
event_times = simulate_poisson_events(event_probability)
print(f"Event times (minutes): {event_times}")
print(f"Number of events: {len(event_times)}")