# Grabs the groupedState files from https://visitdata.org/data and concatenates the files

rm './visitdata-grouped.csv'
touch './visitdata-grouped.csv'

# Get all of and keep header from Illinois
echo "Getting visit data for Illinois"
wget -q "https://visitdata.org/data/groupedIllinois.csv" -O "./Illinois.csv"
cat "./Illinois.csv" >> './visitdata-grouped.csv'
rm "./Illinois.csv"

## for many states
# states="Alabama Alaska Arizona Arkansas California Colorado Connecticut Delaware Florida Georgia Hawaii Idaho Illinois Indiana Iowa Kansas Kentucky Louisiana Maine Maryland Massachusetts Michigan Minnesota Mississippi Missouri Montana Nebraska Nevada NewHampshire NewJersey NewMexico NewYork NorthCarolina NorthDakota Ohio Oklahoma Oregon Pennsylvania RhodeIsland SouthCarolina SouthDakota Tennessee Texas Utah Vermont Virginia Washington WestVirginia Wisconsin Wyoming"
# for state in $states;
# do
#   if [ state != "Illinois" ]; then
#       echo ${state}
#       wget -q "https://visitdata.org/data/grouped${state}.csv" -O "${state}.csv"
#       tail -n +2 "${state}.csv" >> './visitdata-grouped.csv'
#       rm "${state}.csv"
#   fi
# done
