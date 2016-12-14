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
    for root, directories, files in os.walk(location):
        for filename in files:
            # name_new_arch = root + ".txt"
            archv = os.path.join(root, filename)
            name_x_semestre_seg1 = root.split("/")
            name_x_semestre_seg2 = name_x_semestre_seg1[7].split(" ")
            print(root)
            # semestre 1
            if int(name_x_semestre_seg2[0]) < 7:
                name1 = "Union-X-Semestre1.txt"
                archv2 = os.path.join(location, name1)  # para unir por mes ....
                if archv.endswith(".csv"):
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
            # semestre 2
            else:
                name2 = "Union-X-Semestre2.txt"
                archv2 = os.path.join(location, name2)  # para unir por mes ....
                if archv.endswith(".csv"):
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
