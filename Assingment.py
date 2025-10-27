#Example: Find vehicles that violated the speed limit
speeds = [45, 67, 89, 54, 120, 75]
speed_limit = 60

violators = [speed for speed in speeds if speed > speed_limit]
print("Vehicles that violated the speed limit:", violators)
# Example: Sort vehicle speeds
speeds = [45, 67, 89, 54, 120, 75]
sorted_speeds = sorted(speeds)
print("Sorted speeds:", sorted_speeds)
# Example: Count how many vehicles violated the speed limit
speeds = [45, 67, 89, 54, 120, 75]
speed_limit = 60

violations = sum(speed > speed_limit for speed in speeds)
print("Number of vehicles that violated the speed limit:", violations)
