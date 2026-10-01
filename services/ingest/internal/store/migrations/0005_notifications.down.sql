DROP TABLE notification_channels;
DROP TABLE notification_deliveries;
DROP TABLE alerts;
DROP TABLE forecast_observations;
DROP TABLE forecast_sites;

DROP INDEX devices_site_key_idx;
ALTER TABLE devices DROP COLUMN site_key;
ALTER TABLE devices DROP CONSTRAINT devices_latlon_together;
ALTER TABLE devices DROP COLUMN longitude;
ALTER TABLE devices DROP COLUMN latitude;
