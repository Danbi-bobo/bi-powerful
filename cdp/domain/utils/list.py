def split_list(data, max_items_per_sublist):
    sublists = []
    current_sublist = []
    for item in data:
        current_sublist.append(item)
        if len(current_sublist) == max_items_per_sublist:
            sublists.append(current_sublist)
            current_sublist = []
    # Add the remaining items to the last sublist
    if current_sublist:
        sublists.append(current_sublist)
    return sublists
