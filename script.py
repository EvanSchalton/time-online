import os
from collections import defaultdict
from typing import List
from contextlib import closing
import csv
import codecs
import requests
from tqdm import tqdm

root_url = "https://public.wiwdata.com/engineering-challenge/data"
output_file = "results.csv"

A_CHR_NUMBER = 97
LETTERS = 26
DEFAULT_FILES = ",".join(
    [f"{chr(i)}.csv" for i in range(A_CHR_NUMBER, A_CHR_NUMBER + LETTERS)]
)


def csv_gen(url: str):
    """
    Lazy loads online csvs one line at a time
    """
    with closing(requests.get(url, stream=True)) as r:
        reader = csv.reader(
            codecs.iterdecode(r.iter_lines(), "ascii"),
            delimiter=",",
            quotechar='"',
        )
        header = None
        for index, row in enumerate(reader):
            if index == 0:
                header = row
                continue
            yield dict(zip(header, row))


def download_data(
    root_url: str,
    files: List[str],
    key: str = "user_id",
    pivot: str = "path",
    target: str = "length",
):
    """
    Downloads all of the given files (assumes same format) and
    aggregates their pivot values
    """
    results = {}
    unqiue_pivots = set()
    for csv_file in tqdm(files):
        url = os.path.join(root_url, csv_file)
        gen = csv_gen(url)
        for data in gen:
            unqiue_pivots.add(data[pivot])
            results.setdefault(int(data[key]), defaultdict(lambda: 0))[
                data[pivot]
            ] += int(data[target])

    sorted_pivots = list(unqiue_pivots)
    sorted_pivots.sort()

    return {"results": results, "pivots": sorted_pivots}


def save_data(
    results: dict, pivots: List[str], output_file: str, key: str = "user_id"
):
    """
    Writes the results as a csv to the provided output_file
    """
    sorted_users = list(results.keys())
    sorted_users.sort()

    with open(output_file, "w+") as out_file:
        out_file.write(",".join([key] + pivots) + "\n")

        for user in sorted_users:
            path_dict = results[user]
            out_file.write(
                ",".join(
                    [str(user)] + [str(path_dict[path]) for path in pivots]
                )
                + "\n"
            )


def default_input(label, default_value):
    """
    Utiltiy function to provide a default value to the built-in input
    """
    response = input(f"{label} [{default_value}]: ")
    response = response or default_value
    return response


def main():
    """
    Control function with the script is invoked directly
    """
    root_url = default_input(
        "Root URL", "https://public.wiwdata.com/engineering-challenge/data"
    )
    files_string = default_input("CSV Files", DEFAULT_FILES)
    files = [f.strip() for f in files_string.split(",")]
    output_file = default_input("Save As", "results.csv")
    key = default_input("Key", "user_id")
    pivot = default_input("Pivot", "path")
    target = default_input("Target", "length")

    downloads = download_data(
        root_url=root_url, files=files, key=key, pivot=pivot, target=target
    )
    save_data(downloads["results"], downloads["pivots"], output_file, key=key)


if __name__ == "__main__":
    main()
