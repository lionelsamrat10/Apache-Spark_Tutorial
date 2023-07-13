from functools import reduce

nums = [3,2,6,8,4,6,2,9]

# get even numbers
# using filter function
evens = list(filter(lambda x : x % 2 == 0, nums))

# map function
doubled_nums = list(map(lambda x : x * 2,evens))

# reduce function
sum = reduce(lambda x, y : x + y ,doubled_nums)


print(evens)
print(doubled_nums)
print(sum)
