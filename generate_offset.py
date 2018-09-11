import numpy as np
import dask.dataframe as ddf
import sys
import csv

lat_lng_1 = sys.argv[1]
lat_lng_2 = sys.argv[2]
map_data = sys.argv[3]
map_pos = []
latlng_pos = []
earth_radius = 6378.137

df_1 = ddf.read_csv(lat_lng_1)
df_2 = ddf.read_csv(lat_lng_2)


def latlng_to_diff(latlng_pos):
    diff_x = earth_radius * (np.radians(latlng_pos[0][1]) - np.radians(latlng_pos[1][1])) * 1000
    diff_y = earth_radius * (np.radians(latlng_pos[0][0]) - np.radians(latlng_pos[1][0])) * 1000

    return [diff_x, diff_y]


def calc_offset(latlng_pos, map_pos):
    dist_latlng = latlng_to_diff(latlng_pos)
    dist_map = np.array(map_pos[0]) - np.array(map_pos[1])
    l_latlng = np.linalg.norm(dist_latlng)
    l_map = np.linalg.norm(dist_map)

    dist_offset = l_map / l_latlng
    angle_offset = (np.arctan(dist_map[1]/dist_map[0]) - np.arctan(dist_latlng[1] / dist_latlng[0])) * 180 / np.pi

    return dist_offset, angle_offset


with open(map_data, 'r') as f:
    reader = csv.reader(f)
    header = next(reader)

    for row in reader:
        row = [float(s) for s in row]
        map_pos.append(row)


ave_1 = df_1.mean().compute()
ave_2 = df_2.mean().compute()

latlng_pos.append([ave_1[0], ave_1[1]])
latlng_pos.append([ave_2[0], ave_2[1]])

dist_offset, angle_offset = calc_offset(latlng_pos, map_pos)

print("Distance Offset : " + str(dist_offset))
print("Angle Offset : " + str(angle_offset))
print("Average1 : \n" + str(ave_1))
print("Average2 : \n" + str(ave_2))

with open("offset.txt", 'w') as f:
   f.write("Distance Offset\nAngle Offset\n")
   f.write("------------------------------\n")
   f.write(str(dist_offset) + "\n")
   f.write(str(angle_offset) + "\n")
   f.write("------------------------------\n")
   f.write("------------------------------\n")
   f.write("Average\n")
   f.write("------------------------------\n")
   f.write("lat, lng\n")
   f.write(str(ave_1[0]) + "," + str(ave_1[1]) + "\n")
   f.write(str(ave_2[0]) + "," + str(ave_2[1]) + "\n")



