-- Code to create a Big Query table that will contain the crawled data
CREATE TABLE `web-scraping-371310.crawled_datasets.lukas_mobile_de` (
  marke STRING OPTIONS (description = 'The car brand'),
  modell STRING OPTIONS (description = 'The model of the car brand'),
  variante STRING OPTIONS (description = ''),
  titel STRING OPTIONS (description = 'The title of the listing'),
  form STRING OPTIONS (description = 'The type of the vehicle (e.g., Cabrio / Roadster)'),
  fahrzeugzustand STRING OPTIONS (description = 'The state of the vehicle (e.g., Unfallfrei)'),
  leistung STRING OPTIONS (description = 'The horse power of the vehicle'),
  getriebe STRING OPTIONS (description = 'The type of transmission (e.g., automatic)'),
  farbe STRING OPTIONS (description = 'The color of the vehicle'),
  preis INT64 OPTIONS (description = 'The price of the vehicle'),
  kilometer FLOAT64 OPTIONS (description = 'The number of kilometers the car has travelled'),
  erstzulassung STRING OPTIONS (description = 'The first registration of the vehicle'),
  fahrzeughalter FLOAT64 OPTIONS (description = 'Number of passengers'),
  standort STRING OPTIONS (description = 'Place of advertisement'),
  fahrzeugbescheibung STRING OPTIONS (description = 'Vehicle description'),
  url_to_crawl STRING OPTIONS (description = 'The URL of the car on mobile.de'),
  page_rank INT64 OPTIONS (description = 'Position of the car on the listing page'),
  total_num_pages INT64 OPTIONS (description = 'Total number of listing pages under that car category'),
  crawled_timestamp TIMESTAMP OPTIONS (description = 'The timestamp when the listing was crawled')
)