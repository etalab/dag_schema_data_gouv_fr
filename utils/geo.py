from typing import List
import geojson
import json
import os
import pandas as pd
import requests
from shapely.geometry import Point, shape
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

with open("/opt/airflow/dags/dag_schema_data_gouv_fr/utils/france_bbox.geojson") as f:
    FRANCE_BBOXES = geojson.load(f)

def is_point_in_polygon(x: float, y: float, polygon: List[List[float]]) -> bool:
    point = Point(x, y)
    polygon_shape = Polygon(polygon)
    return polygon_shape.contains(point)


def is_point_in_france(coordonnees_xy: List[float]) -> bool:
    p = Point(*coordonnees_xy)
    
    # Create a Polygon
    geoms = [region["geometry"] for region in FRANCE_BBOXES.get('features')]
    polys = [shape(geom) for geom in geoms]
    return any([p.within(poly) for poly in polys])


def fix_coordinates_order(filepath: List[str], coordinates_column: str="coordonneesXY") -> None:
    """
    Cette fonction modifie un fichier CSV pour placer la longitude avant la latitude
    dans la colonne qui contient les deux au format "[lon, lat]".
    """

    def fix_coordinates(row: pd.Series) -> pd.Series:
        coordonnees_xy = json.loads(row[coordinates_column])
        reversed_coordonnees = list(reversed(coordonnees_xy))
        if is_point_in_france(reversed_coordonnees):
            # Coordinates are inverted with lat before lon
            row[coordinates_column] = json.dumps(reversed_coordonnees)
            fix_coordinates.rows_modified = fix_coordinates.rows_modified + 1
        return row

    for filepath in filepaths:
        fix_coordinates.rows_modified = 0
        source_df = pd.read_csv(filepath)
        source_df.apply(fix_coordinates, axis=1).to_csv(filepath, index=False)
        print(f"Rows modified: {fix_coordinates.rows_modified}/{len(source_df)}")


def create_lon_lat_cols(filepaths: List[str], coordinates_column: str="coordonneesXY") -> None:
    """Add longitude and latitude columns to CSV using coordinates_column"""
    for filepath in filepaths:
        df = pd.read_csv(filepath)
        coordinates = df[coordinates_column].apply(json.loads)
        df['longitude'] = coordinates.str[0]
        df['latitude'] = coordinates.str[1]
        df.to_csv(filepath)


def export_to_geojson(filepaths: List[str], coordinates_column: str="coordonneesXY") -> None:
    """Convert CSV into Geojson format"""
    for filepath in filepaths:
        df = pd.read_csv(filepath)

        json_result_string = df.to_json(
            orient='records',
            double_precision=12,
            date_format='iso'
        )
        json_result = json.loads(json_result_string)

        geojson = {
            'type': 'FeatureCollection',
            'features': []
        }
        for record in json_result:
            coordinates = json.loads(record[coordinates_column])
            longitude, latitude = coordinates
            geojson['features'].append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [longitude, latitude],
                },
                'properties': record,
            })
        geojson_filepath = os.path.splitext(filepath)[0] + '.json'
        with open(geojson_filepath, 'w') as f:
            f.write(json.dumps(geojson, indent=2))


def fix_code_insee(filepaths: List[str], code_insee_col: str='code_insee_commune', address_col: str='adresse_station', lon_col: str='longitude', lat_col: str='latitude'):
    """Check code INSEE in CSV file and enrich with postcode and city
    Requires address and coordinates columns
    """
    def enrich_row_address(row: pd.Series) -> pd.Series:
        # Try getting code INSEE from address (ordered by latitude and longitude)
        address_url = f'https://api-adresse.data.gouv.fr/search/?q={row[address_col]}&lon={row[lon_col]}&lat={row[lat_col]}'
        results = requests.get(url=address_url)
        print(address_url)
        all_addresses = json.loads(results.content)['features']
        if len(all_addresses) > 0:
            most_likely_address = all_addresses[0]['properties']
            row[code_insee_col] = most_likely_address['citycode']
            row['code_postal'] = most_likely_address['postcode']
            row['ville'] = most_likely_address['city']
        else:
            # Try getting commune with code INSEE from latitude and longitude alone
            commune_results = json.loads(requests.get(url=f'https://geo.api.gouv.fr/communes?lat={row[lat_col]}&lon={row[lon_col]}&fields=code,nom,codesPostaux').content)
            print(commune_results)
            if len(commune_results) > 0:
                commune = commune_results[0]
                row[code_insee_col] = commune['code']
                if len(commune['codesPostaux']) > 1:
                    print(f'Attention: plusieurs code postaux pour {row}')
                row['code_postal'] = commune['codesPostaux'][0]
                row['ville'] = commune['nom']
            else:
                print(f'Attention: adresse non enrichie: {row}')
                row['code_postal'] = ''
                row['ville'] = ''
        return row

    for filepath in filepaths:
        pd.read_csv(filepath).apply(enrich_row_address, axis=1).to_csv(os.path.splitext(filepath)[0]+'_test.csv')
    