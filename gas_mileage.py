# Simple Python program to calculate gas mileage

def main():
    # Ask user for input
    miles = float(input("Enter miles driven: "))
    gas = float(input("Enter gas used (in gallons): "))
    
    # Calculate mileage
    if gas == 0:
        print("Gas used cannot be zero.")
    else:
        mileage = miles / gas
        print(f"Your mileage is {mileage:.2f} miles per gallon (MPG).")

if __name__ == "__main__":
    main()
