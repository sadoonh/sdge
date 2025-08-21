-- production_dim_zip_city_state_update.sql
BEGIN;

-- 1) Ensure columns exist
ALTER TABLE production.dim_zip
  ADD COLUMN IF NOT EXISTS city  text;
ALTER TABLE production.dim_zip
  ADD COLUMN IF NOT EXISTS state char(2);

-- 2) Ensure these ZIP codes exist (won't touch existing ones)
INSERT INTO production.dim_zip (zip_code) VALUES
('91931'),('92024'),('92694'),('92070'),('91978'),('91945'),('92026'),('91913'),
('92061'),('92019'),('92007'),('92120'),('91901'),('92158'),('92128'),('92096'),
('92154'),('92010'),('92127'),('92126'),('92118'),('92029'),('91962'),('92064'),
('92049'),('92624'),('92117'),('91914'),('92152'),('92536'),('92115'),('92107'),
('92102'),('92104'),('92121'),('92081'),('91948'),('92155'),('92688'),('92199'),
('92079'),('92101'),('92123'),('92674'),('92021'),('92112'),('92084'),('92040'),
('91911'),('92673'),('92075'),('92027'),('91915'),('92091'),('92672'),('92653'),
('92173'),('91912'),('92656'),('92057'),('92130'),('92045'),('92131'),('92690'),
('92009'),('92037'),('92129'),('92055'),('92111'),('92675'),('92060'),('92008'),
('92105'),('92054'),('92014'),('92066'),('92693'),('92086'),('91942'),('92161'),
('91906'),('92676'),('91917'),('92103'),('92082'),('92083'),('91910'),('92679'),
('92071'),('92132'),('92020'),('92058'),('92692'),('92179'),('92067'),('91980'),
('92139'),('92108'),('92116'),('92011'),('91934'),('92654'),('92145'),('92092'),
('92629'),('92036'),('92072'),('91935'),('92106'),('92056'),('92182'),('92110'),
('92691'),('91905'),('92136'),('92025'),('92109'),('92078'),('92114'),('92134'),
('92119'),('92113'),('92651'),('92059'),('91963'),('91902'),('92135'),('92085'),
('92028'),('91941'),('91916'),('92093'),('92068'),('92122'),('91977'),('92677'),
('92625'),('92065'),('92069'),('92124'),('92649'),('92004'),('92003'),('91950'),
('91932')
ON CONFLICT (zip_code) DO NOTHING;

-- 3) Map city/state by ZIP and update
WITH m(zip_code, city, state) AS (
  VALUES
    ('91931','Guatay','CA'),('92024','Encinitas','CA'),('92694','Ladera Ranch','CA'),
    ('92070','Santa Ysabel','CA'),('91978','Spring Valley','CA'),('91945','Lemon Grove','CA'),
    ('92026','Escondido','CA'),('91913','Chula Vista','CA'),('92061','Pauma Valley','CA'),
    ('92019','El Cajon','CA'),('92007','Cardiff By The Sea','CA'),('92120','San Diego','CA'),
    ('91901','Alpine','CA'),('92158','San Diego','CA'),('92128','San Diego','CA'),
    ('92096','San Marcos','CA'),('92154','San Diego','CA'),('92010','Carlsbad','CA'),
    ('92127','San Diego','CA'),('92126','San Diego','CA'),('92118','Coronado','CA'),
    ('92029','Escondido','CA'),('91962','Pine Valley','CA'),('92064','Poway','CA'),
    ('92049','Oceanside','CA'),('92624','Capistrano Beach','CA'),('92117','San Diego','CA'),
    ('91914','Chula Vista','CA'),('92152','San Diego','CA'),('92536','Aguanga','CA'),
    ('92115','San Diego','CA'),('92107','San Diego','CA'),('92102','San Diego','CA'),
    ('92104','San Diego','CA'),('92121','San Diego','CA'),('92081','Vista','CA'),
    ('91948','Mount Laguna','CA'),('92155','San Diego','CA'),('92688','Rancho Santa Margarita','CA'),
    ('92199','San Diego','CA'),('92079','San Marcos','CA'),('92101','San Diego','CA'),
    ('92123','San Diego','CA'),('92674','San Clemente','CA'),('92021','El Cajon','CA'),
    ('92112','San Diego','CA'),('92084','Vista','CA'),('92040','Lakeside','CA'),
    ('91911','Chula Vista','CA'),('92673','San Clemente','CA'),('92075','Solana Beach','CA'),
    ('92027','Escondido','CA'),('91915','Chula Vista','CA'),('92091','Rancho Santa Fe','CA'),
    ('92672','San Clemente','CA'),('92653','Laguna Hills','CA'),('92173','San Ysidro','CA'),
    ('91912','Chula Vista','CA'),('92656','Aliso Viejo','CA'),('92057','Oceanside','CA'),
    ('92130','San Diego','CA'),
    -- 92045 intentionally left unmapped (rare/legacy); will remain NULL
    ('92131','San Diego','CA'),('92690','Mission Viejo','CA'),('92009','Carlsbad','CA'),
    ('92037','La Jolla','CA'),('92129','San Diego','CA'),('92055','Camp Pendleton','CA'),
    ('92111','San Diego','CA'),('92675','San Juan Capistrano','CA'),('92060','Palomar Mountain','CA'),
    ('92008','Carlsbad','CA'),('92105','San Diego','CA'),('92054','Oceanside','CA'),
    ('92014','Del Mar','CA'),('92066','Ranchita','CA'),('92693','San Juan Capistrano','CA'),
    ('92086','Warner Springs','CA'),('91942','La Mesa','CA'),('92161','San Diego','CA'),
    ('91906','Boulevard','CA'),('92676','Silverado','CA'),('91917','Dulzura','CA'),
    ('92103','San Diego','CA'),('92082','Valley Center','CA'),('92083','Vista','CA'),
    ('91910','Chula Vista','CA'),('92679','Trabuco Canyon','CA'),('92071','Santee','CA'),
    ('92132','San Diego','CA'),('92020','El Cajon','CA'),('92058','Oceanside','CA'),
    ('92692','Mission Viejo','CA'),('92179','San Ysidro','CA'),('92067','Rancho Santa Fe','CA'),
    ('91980','Tecate','CA'),('92139','San Diego','CA'),('92108','San Diego','CA'),
    ('92116','San Diego','CA'),('92011','Carlsbad','CA'),('91934','Jacumba','CA'),
    ('92654','Laguna Hills','CA'),('92145','San Diego','CA'),('92092','La Jolla','CA'),
    ('92629','Dana Point','CA'),('92036','Julian','CA'),('92072','Santee','CA'),
    ('91935','Jamul','CA'),('92106','San Diego','CA'),('92056','Oceanside','CA'),
    ('92182','San Diego','CA'),('92110','San Diego','CA'),('92691','Mission Viejo','CA'),
    ('91905','Campo','CA'),('92136','San Diego','CA'),('92025','Escondido','CA'),
    ('92109','San Diego','CA'),('92078','San Marcos','CA'),('92114','San Diego','CA'),
    ('92134','San Diego','CA'),('92119','San Diego','CA'),('92113','San Diego','CA'),
    ('92651','Laguna Beach','CA'),('92059','Pala','CA'),('91963','Potrero','CA'),
    ('91902','Bonita','CA'),('92135','San Diego','CA'),('92085','Vista','CA'),
    ('92028','Fallbrook','CA'),('91941','La Mesa','CA'),('91916','Descanso','CA'),
    ('92093','La Jolla','CA'),('92068','San Luis Rey','CA'),('92122','San Diego','CA'),
    ('91977','Spring Valley','CA'),('92677','Laguna Niguel','CA'),('92625','Corona Del Mar','CA'),
    ('92065','Ramona','CA'),('92069','San Marcos','CA'),('92124','San Diego','CA'),
    ('92649','Huntington Beach','CA'),('92004','Borrego Springs','CA'),('92003','Bonsall','CA'),
    ('91950','National City','CA'),('91932','Imperial Beach','CA')
)
UPDATE production.dim_zip z
SET city  = m.city,
    state = m.state
FROM m
WHERE z.zip_code = m.zip_code;

-- 4) (Optional) See any ZIPs that remain unmapped (e.g., 92045)
-- SELECT zip_code FROM production.dim_zip WHERE city IS NULL OR state IS NULL ORDER BY zip_code;

COMMIT;
