-- =============================================================================
-- CONSULTA EJERCICIO 3: INSIGHTS
-- =============================================================================

-- INSIGHT 1: Discrepancia entre el Índice Big Mac y el Costo de Vida Turístico

SELECT 
    pais, 
    continente, 
    precio_big_mac_usd,
    (hospedaje_bajo_usd + comida_bajo_usd + transporte_bajo_usd) AS presupuesto_diario_mochilero
FROM 
    warehouse.paises_turismo
WHERE 
    hospedaje_bajo_usd IS NOT NULL
ORDER BY 
    precio_big_mac_usd DESC, 
    presupuesto_diario_mochilero ASC;


-- INSIGHT 2: Oportunidades de Mayores

SELECT 
    pais, 
    continente, 
    tasa_de_envejecimiento, 
    entretenimiento_promedio_usd,
    hospedaje_promedio_usd
FROM 
    warehouse.paises_turismo
WHERE 
    tasa_de_envejecimiento > 20 
    AND entretenimiento_promedio_usd < 30 
ORDER BY 
    tasa_de_envejecimiento DESC;


-- INSIGHT 3: Identificación de Oportunidades

SELECT 
    pais, 
    poblacion, 
    precio_big_mac_usd, 
    (hospedaje_bajo_usd + comida_bajo_usd + transporte_bajo_usd + entretenimiento_bajo_usd) AS costo_total_vida_bajo
FROM 
    warehouse.paises_turismo
WHERE 
    poblacion > 5000000 
    AND precio_big_mac_usd < 2.50
ORDER BY 
    costo_total_vida_bajo ASC;