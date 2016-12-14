#!/usr/bin/env python3

import os
import sys
# import os.path

if __name__ == "__main__":
    dataset = sys.argv[1]
    script_location = os.path.abspath(os.getcwd())

    if not os.path.isabs(dataset):
        location = os.path.join(script_location, dataset)
    else:
        location = dataset

    if not os.path.exists(location):
        print("Input folder", location, "does not exist")
        sys.exit(1)
    name = "Union-X-Mes.txt"
    for root, directories, files in os.walk(location):
        for filename in files:
            # name_new_arch = root + ".txt"
            archv = os.path.join(root, filename)
            print(root + "/" + filename)
            if archv.endswith(".csv"):
                archv2 = os.path.join(root, name)  # para unir por mes ....
                with open(archv, "r") as f:
                    with open(archv2, "a") as f3:
                        lines = f.read().splitlines()
                        tam = 0
                        total = 0
                        for line in lines:
                            line = line.strip()
                            if line[0] != "#":
                                tam = tam + 1
                                date, value = line.split(",")
                                # total = total + float(value)
                                f3.write("{}\n".format(value))


            # date = filename.replace("UTP_NAA_", "").replace(".csv", "")

            # f2.write("{}, {}\n".format(date, total / float(tam)))
            # print(archv)
            #print("SUM: ", total)
            #print("TAM: ", tam)
            #print("Prom: ", (total / float(tam)))
