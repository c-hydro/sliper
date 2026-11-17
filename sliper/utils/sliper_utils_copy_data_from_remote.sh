# Remote configuration
soilslips_op="admin@10.24.193.158"

# Source and destination
SRC="/home/admin/DB/data/soil_moisture/2023/"
DST="/home/fabio/soil_moisture/"

# Create local directory
mkdir -p "${DST}"

# Sync remote to local
rsync -avhP \
    -e ssh \
    "${soilslips_op}:${SRC}" \
    "${DST}"
