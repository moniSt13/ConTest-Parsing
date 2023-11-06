def get_all_combinations(rows: list, accessor) -> list[list]:
    from collections import Counter
    from itertools import combinations

    unique_items = Counter(map(lambda x: x[accessor], rows))
    len_count = len(unique_items.keys())

    print(f"Row Count (n): {len(rows)}")
    print(f"Lenght of Combinatorics (r): {len_count}")

    comb = combinations(rows, len_count)

    finished_combinations = []
    for item in comb:
        names = list(dict.fromkeys(list(map(lambda x: x[accessor], item))))
        if len(names) == len_count:
            finished_combinations.append(list(item))

    print(f"finished combinatorics")
    return finished_combinations
