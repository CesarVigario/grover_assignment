def function(max_rows:int = 5, number_of_balls: int = 0) -> int:
    """Returns the number of rows filled depending on the number of balls and the triangle shape."""
    if number_of_balls == 0:
        print("Number of balls exceeds the triangle space.")
        return None
    row = 1
    for i in list(range(max_rows+1, 0, -1)):
        if sum(list(range(i, max_rows+1))) < number_of_balls <= sum(list(range(i-1, max_rows+1))):
            #imagine number_of_balls = 8, and max_rows = 5
            # i would first be 6 -> so range(6, 6) which is zero < number_of_balls <= 0 -> sum([5]) -> because 8 is higher
            # i goes to be 5 -> sum([5]) < number_of_balls <= sum([4, 5]) which is 9 -> yeah 8 is in here! return row!
            return row
        row += 1
        if row > max_rows:
            print("Number of balls exceeds the triangle space.")
            return None

print(function(max_rows=5, number_of_balls=8))