def to_date(number):
    x = number/60%24
    decimal = int((x-int(str(x).split('.')[0]))*60)
    if len(str(decimal))==1:
        decimal = f"0{decimal}"
    return f"{int(x)}:{decimal}"