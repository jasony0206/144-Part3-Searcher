CREATE TABLE ItemLocationPoint (
	ItemID varchar(10) NOT NULL,
	LocationPoint point NOT NULL,
	primary key (ItemID)
) ENGINE = MYISAM;

insert into ItemLocationPoint (ItemID, LocationPoint)
select ItemID, Point(Latitude, Longitude)
from Items i, LocationInfo l
where i.Location = l.Location 
and i.Country = l.Country
and Latitude is not NULL
and Longitude is not NULL;

CREATE SPATIAL INDEX locationIndex ON ItemLocationPoint (LocationPoint);