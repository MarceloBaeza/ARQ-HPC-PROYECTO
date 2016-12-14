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

            # Invierno
            if int(name_x_semestre_seg2[0]) == 12 or int(name_x_semestre_seg2[0]) == 1 or int(name_x_semestre_seg2[0]) == 2:
                name1 = "Union-X-Invierno.txt"
                # para unir por mes ....
                archv2 = os.path.join(location, name1)
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
            # Primavera
            if int(name_x_semestre_seg2[0]) == 3 or int(name_x_semestre_seg2[0]) == 4 or int(name_x_semestre_seg2[0]) == 5:
                name1 = "Union-X-Primavero.txt"
                # para unir por mes ....
                archv2 = os.path.join(location, name1)
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
            # Otoño
            if int(name_x_semestre_seg2[0]) == 9 or int(name_x_semestre_seg2[0]) == 10 or int(name_x_semestre_seg2[0]) == 11:
                name1 = "Union-X-Otono.txt"
                # para unir por mes ....
                archv2 = os.path.join(location, name1)
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
            # verano
            if int(name_x_semestre_seg2[0]) == 6 or int(name_x_semestre_seg2[0]) == 7 or int(name_x_semestre_seg2[0]) == 8:
                name1 = "Union-X-Verano.txt"
                # para unir por mes ....
                archv2 = os.path.join(location, name1)
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
