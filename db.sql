CREATE TABLE pais_poblacion (
    _id TEXT PRIMARY KEY,
    continente TEXT,
    pais TEXT,
    poblacion BIGINT,
    costo_bajo_hospedaje DECIMAL,
    costo_promedio_comida DECIMAL,
    costo_bajo_transporte DECIMAL,
    costo_promedio_entretenimiento DECIMAL
);

CREATE TABLE pais_envejecimiento (
    id_pais INTEGER PRIMARY KEY,
    nombre_pais TEXT,
    capital TEXT,
    continente TEXT,
    region TEXT,
    poblacion DECIMAL,
    tasa_de_envejecimiento DECIMAL
);


COPY pais_poblacion
FROM 'C:/Users/Public/pais_poblacion.csv'
DELIMITER ';'
CSV HEADER
ENCODING 'UTF8';


COPY pais_envejecimiento
FROM 'C:/Users/Public/pais_envejecimiento.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

