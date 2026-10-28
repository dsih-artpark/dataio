-- Migration 027: Karnataka regions for the KGIS 2026 boundaries
-- Regions the KGIS (KSRSAC) boundary set needs, and Karnataka regions in the
-- latest LGD (2026-10-28) that the regions table doesn't have yet. IDs follow
-- the existing <type>_<LGD code> scheme; names are LGD's English names, as for
-- the existing rows. None of these existed before. Re-running is harmless.

BEGIN;

INSERT INTO regions (region_id, region_name, parent_region_id)
VALUES
    -- Taluks and a village new in KGIS and LGD. Villages KGIS places under the
    -- new taluks keep their current parents here.
    ('subdistrict_7398', 'Terdal', 'district_524'),
    ('subdistrict_7403', 'Arakera', 'district_546'),
    ('subdistrict_7404', 'Manchenahalli', 'district_630'),
    ('subdistrict_7407', 'Chelur', 'district_630'),
    ('village_948414', 'D Salundi', 'subdistrict_5572'),
    -- ULBs: 243 existing ward rows (e.g. ward_251818-1 'Gokak Ward No. 1') have
    -- one of these as parent_region_id, but the ULB rows were never added, so
    -- those wards hang off nothing. Names follow the existing ULB rows
    -- ("<name> <local body type>"); the ward rows are not changed.
    ('ulb_251818', 'Gokak City Municipal Council', 'district_527'),
    ('ulb_251828', 'Saundatti-Yellamma City Municipal Council', 'district_527'),
    ('ulb_251829', 'Ramdurg Town Municipal Council', 'district_527'),
    ('ulb_251835', 'Mudhol City Municipal Council', 'district_524'),
    ('ulb_251868', 'Homnabad Town Municipal Council', 'district_529'),
    ('ulb_251964', 'Kadur Town Municipal Council', 'district_532'),
    ('ulb_251981', 'Bagepalli Town Municipal Council', 'district_630'),
    ('ulb_252029', 'Someshwar Town Municipal Council', 'district_534'),
    ('ulb_253188', 'Kanakpura City Municipal Council', 'district_631'),
    -- Inhabited villages in LGD that aren't in the table yet (newer LGD codes,
    -- 948369-962365), under their LGD taluk. KGIS has no boundaries for them yet.
    ('village_948369', 'Jafarwadi', 'subdistrict_5438'),
    ('village_948370', 'Basurte', 'subdistrict_5438'),
    ('village_948371', 'Bendi', 'subdistrict_5438'),
    ('village_948372', 'Devagiri', 'subdistrict_5438'),
    ('village_948373', 'Dharanahatti', 'subdistrict_5438'),
    ('village_948374', 'Janewadi', 'subdistrict_5438'),
    ('village_948375', 'Karvinakumpi', 'subdistrict_5438'),
    ('village_948376', 'Kavlewadi', 'subdistrict_5438'),
    ('village_948392', 'Gandhinagar A', 'subdistrict_5555'),
    ('village_948393', 'Gandhinagar K', 'subdistrict_5555'),
    ('village_948394', 'Gandhinagar', 'subdistrict_5555'),
    ('village_948395', 'Gopalpura', 'subdistrict_5555'),
    ('village_948396', 'Indiranagar', 'subdistrict_5555'),
    ('village_948397', 'Jenukal Nagar', 'subdistrict_5555'),
    ('village_948398', 'Laxmipura', 'subdistrict_5555'),
    ('village_948399', 'Laxmipura B', 'subdistrict_5555'),
    ('village_948400', 'Mayanagar', 'subdistrict_5555'),
    ('village_948401', 'Nehrunagar', 'subdistrict_5555'),
    ('village_948402', 'Ranganathpura', 'subdistrict_5555'),
    ('village_948403', 'Indiranagar M', 'subdistrict_5555'),
    ('village_948404', 'M Mattihalli', 'subdistrict_5555'),
    ('village_948405', 'Seva Siddeshwar Nagar', 'subdistrict_5555'),
    ('village_948406', 'Sreenivasanagar', 'subdistrict_5555'),
    ('village_948407', 'Vijaya Nagar', 'subdistrict_5555'),
    ('village_948412', 'Basavarajapura', 'subdistrict_5514'),
    ('village_948413', 'Basaveshwaranagar', 'subdistrict_5514'),
    ('village_948415', 'Hosabannihatti', 'subdistrict_5514'),
    ('village_948416', 'Kadarappanahatti', 'subdistrict_5512'),
    ('village_948417', 'Kasturibanagar', 'subdistrict_5514'),
    ('village_948418', 'Shivakulenooru', 'subdistrict_5514'),
    ('village_948550', 'Chikkokatanur', 'subdistrict_5451'),
    ('village_948551', 'Sevalaya Nagar', 'subdistrict_5494'),
    ('village_948552', 'Indira Nagar', 'subdistrict_7071'),
    ('village_948553', 'Tirupatinagar', 'subdistrict_7071'),
    ('village_948554', 'Dinne Soluru', 'subdistrict_5603'),
    ('village_948555', 'Koira Hosur', 'subdistrict_5603'),
    ('village_948556', 'P Ranganathpura', 'subdistrict_5603'),
    ('village_948557', 'Bandenahalli', 'subdistrict_5535'),
    ('village_948558', 'Seekayanahalli', 'subdistrict_5603'),
    ('village_948559', 'Singawara', 'subdistrict_5603'),
    ('village_948560', 'Bodarahalli', 'subdistrict_5534'),
    ('village_948561', 'Lingadahalli', 'subdistrict_5534'),
    ('village_948562', 'Megalapalya', 'subdistrict_5534'),
    ('village_948563', 'Roppa', 'subdistrict_5534'),
    ('village_948564', 'Yellappanayakanahlli', 'subdistrict_5534'),
    ('village_948566', 'Mudigondavabanadinne', 'subdistrict_5596'),
    ('village_948567', 'Yadarlahalli M', 'subdistrict_5596'),
    ('village_949877', 'Issamudra Hosahatti', 'subdistrict_5505'),
    ('village_949878', 'Chikkagondanahalli Gollarahatti', 'subdistrict_5505'),
    ('village_949879', 'Hosahatti', 'subdistrict_5508'),
    ('village_950499', 'Kivadebailu', 'subdistrict_7081'),
    ('village_950500', 'Siddapura', 'subdistrict_7081'),
    ('village_950503', 'Devagiri', 'subdistrict_5511'),
    ('village_950505', 'Gandhinagara', 'subdistrict_5511'),
    ('village_952758', 'Dupenahatti', 'subdistrict_7081'),
    ('village_953560', 'Prabhunagar', 'subdistrict_5439'),
    ('village_955829', 'Benchikatte', 'subdistrict_5511'),
    ('village_960179', 'Kanase', 'subdistrict_5515'),
    ('village_960180', 'Kenchagondanakoppa', 'subdistrict_5515'),
    ('village_960181', 'Sulluru', 'subdistrict_5515'),
    ('village_960182', 'Choonikoppa', 'subdistrict_5515'),
    ('village_962362', 'Malligenagalli', 'subdistrict_7076'),
    ('village_962363', 'Sevapura', 'subdistrict_7076'),
    ('village_962364', 'Mariyammanagara', 'subdistrict_7076'),
    ('village_962365', 'Vijayanagara', 'subdistrict_7076')
ON CONFLICT (region_id) DO NOTHING;

SELECT add_migration(27, '027_regions_karnataka_kgis');

COMMIT;
