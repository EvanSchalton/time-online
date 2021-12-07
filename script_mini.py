import os, csv, codecs, requests
from collections import defaultdict
from typing import List
from contextlib import closing

DEFAULT_FILES = [f"{chr(i)}.csv" for i in range(97, 97 + 26)]


def csv_gen(url: str):
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
    output_file: str,
):
    results = {}
    unqiue_pivots = set()
    for csv_file in DEFAULT_FILES:
        url = os.path.join(root_url, csv_file)
        gen = csv_gen(url)
        for data in gen:
            unqiue_pivots.add(data["path"])
            results.setdefault(int(data["user_id"]), defaultdict(lambda: 0))[
                data["path"]
            ] += int(data["length"])

    paths = list(unqiue_pivots)
    paths.sort()

    sorted_users = list(results.keys())
    sorted_users.sort()

    with open(output_file, "w+") as out_file:
        out_file.write(",".join(["user_id"] + paths) + "\n")

        for user in sorted_users:
            path_dict = results[user]
            out_file.write(
                ",".join(
                    [str(user)] + [str(path_dict[path]) for path in paths]
                )
                + "\n"
            )


if __name__ == "__main__":
    downloads = download_data(input("Root URL: "), input("Save As: "))
