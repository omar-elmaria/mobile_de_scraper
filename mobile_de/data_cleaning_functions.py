# Import packages
import logging
import os

import pandas as pd
from google.cloud import bigquery, bigquery_storage
from google.oauth2 import service_account


class HelperFunctions:
    """
    A class to hold helper functions
    """
    ## General helper functions
    def filter_for_a_specific_brand(self, df, marke, modell):
        """
        A function to filter for a specific brand and model
        """
        # Filter for the marke and modell
        df_specific_brand = df[
            (df["marke"] == marke) & # Brand (e.g., Porsche)
            (df["modell"] == modell) & # Model (e.g., 992)
            (df["standort"].str.lower().str.contains("de")) # Filter for DE as we are only interested in the German market
        ].reset_index(drop=True)

        return df_specific_brand

    def set_bigquery_credentials(self):
        """
        A function to set the BigQuery credentials
        """
        # Set the BigQuery credentials
        key_path_home_dir = os.path.expanduser("~") + "/bq_credentials.json"
        credentials = service_account.Credentials.from_service_account_file(
            key_path_home_dir, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        # Now, instantiate the client and upload the table to BigQuery
        bq_client = bigquery.Client(project="web-scraping-371310", credentials=credentials)
        bqstorage_client = bigquery_storage.BigQueryReadClient(credentials=credentials)

        return bq_client, bqstorage_client
    
    ###------------------------------###------------------------------###

    ## Form column helper functions
    def amend_form_col_porsche_992_gt3(self, x):
        """
        A function to amend the `form` column for Porsche 992 GT3
        """
        if x.find("sportwagen/coup") != -1:
            return "Coupé"
        else:
            return x
    
    def amend_form_col_lamborghini_urus(self, x):
        """
        A function to amend the `form` column for Lamborghini Urus
        """
        if x.lower().find("suv") != -1 or x.lower().find("geländewagen") != -1\
        or x.lower().find("pickup") != -1 or x.lower().find("andere") != -1\
        or x.lower().find("sportwagen") != -1 or x.lower().find("coup") != -1:
            return "Super Sport Utility Vehicle"
        else:
            return x
    
    def amend_form_col_aston_martin_dbx(self, x):
        """
        A function to amend the `form` column for Aston Martin DBX
        """
        if x.lower().find("suv") != -1 or x.lower().find("geländewagen") != -1\
        or x.lower().find("pickup") != -1 or x.lower().find("andere") != -1\
        or x.lower().find("sportwagen") != -1 or x.lower().find("coup") != -1:
            return "SUV"
        else:
            return x
    
    def amend_form_col_mercedes_benz_g_63_amg(self, x):
        """
        A function to amend the `form` column for Mercedes-Benz G 63 AMG
        """
        if x.lower().find("suv") != -1 or x.lower().find("geländewagen") != -1\
        or x.lower().find("pickup") != -1 or x.lower().find("tageszulassung") != -1\
        or x.lower().find("jahreswagen") != -1 or x.lower().find("neufahrzeug") != -1\
        or x.lower().find("vorführfahrzeug") != -1 or x.lower().find("van") != -1\
        or x.lower().find("minibus") != -1 or x.lower().find("sportwagen") != -1\
        or x.lower().find("coup") != -1 or x.lower().find("limousine") != -1 or x.lower().find("andere/neufahrzeug") != -1:
            return "SUV"
        else:
            return x
    
    def amend_form_col_mercedes_benz_sls_amg(self, x):
        """
        A function to amend the `form` column for Mercedes-Benz SLS AMG
        """
        if x.lower().find("cabrio") != -1 or x.lower().find("roadster") != -1:
            return "Roadster"
        elif x.lower().find("sportwagen") != -1 or x.lower().find("coupe") != -1:
            return "Coupe"
        else:
            return x
    
    def amend_form_col_mclaren(self, x):
        """
        A function to amend the `form` column for McLaren 765LT
        """
        if x.lower().find("cabrio") != -1 or x.lower().find("roadster") != -1:
            return "Spider"
        elif x.lower().find("sportwagen") != -1 or x.lower().find("coup") != -1\
        or x.lower().find("tageszulassung") != -1 or x.lower().find("jahreswagen") != -1\
        or x.lower().find("neufahrzeug") != -1 or x.lower().find("vorführfahrzeug") != -1:
            return "Coupe"
        else:
            return x
    
    def amend_form_col_maserati(self, x):
        """
        A function to amend the `form` column for Maserati
        """
        return self.amend_form_col_mclaren(x)
    
    def amend_form_col_ferrari_sf90(self, x):
        """
        A function to amend the `form` column for Ferrari SF90
        """
        if x.lower().find("cabrio") != -1 or x.lower().find("roadster") != -1:
            return "Spider"
        elif x.lower().find("sportwagen") != -1 or x.lower().find("coupe") != -1:
            return "Coupe"
        else:
            return x
    
    def amend_form_col_ferrari_812_stg_1(self, x):
        """
        A function to amend the `form` column for Ferrari 812 (stg 1)
        """
        if x.lower().find("cabrio") != -1 or x.lower().find("roadster") != -1:
            return "Cabrio"
        elif x.lower().find("sportwagen") != -1 or x.lower().find("coupe") != -1:
            return "Coupe"
        else:
            return x
    
    def amend_form_col_ferrari_812_stg_2(self, x):
        """
        A function to amend the `form` column for Ferrari 812 (stg 2)
        """
        if x["variante"] == "812 Competizione A":
            return "Cabrio"
        elif x["variante"] == "812 Competizione":
            return "Coupe"
        else:
            return x["variante"]
    
    def amend_form_col_ferrari_812_stg_3(self, x):
        """
        A function to amend the `form` column for Ferrari 812 (stg 3)
        """
        if x["variante"] == "812 GTS":
            return "Cabrio"
        elif x["variante"] == "812 Superfast":
            return "Coupe"
        else:
            return x["variante"]
    
    def amend_form_col_ferrari_f12_stg_1(self, x):
        """
        A function to amend the `form` column for Ferrari F12 (stg 1)
        """
        if x.lower().find("sportwagen") != -1 or x.lower().find("coupe") != -1:
            return "Coupe"
        else:
            return x
    
    def amend_form_col_ferrari_f12_stg_2(self, x):
        """
        A function to amend the `form` column for Ferrari F12 (stg 2)
        """
        if x["variante"] == "F12 TDF" or x["variante"] == "F12 Berlinetta":
            return "Coupe"
        else:
            return x["variante"]
        
    ###------------------------------###------------------------------###
    
    ## Fahrzeugzustand column helper functions 
    def amend_fahrzeugzustand_col(self, x):
        """
        A function to amend the `fahrzeugzustand` column
        """
        if x == "" or x is None or pd.isnull(x):
            return "Unfallfrei"
        elif x.lower().find("unfallfrei") != -1:
            return "Unfallfrei"
        else:
            return x
        
    ###------------------------------###------------------------------###

    ## Kilometer column helper functions
    def amend_kilometer_col(self, x):
        """
        A function to amend the `kilometer` column
        """
        if x == "" or x is None or pd.isnull(x):
            return 1
        else:
            return x
        
    ###------------------------------###------------------------------###

    ## Getriebe column helper functions
    def amend_getriebe_col_stg_1_porsche_992_gt3(self, x):
        """
        A function to amend the `getriebe` column for Porsche 992 GT3 (stg 1)
        """
        if x == "" or x is None or pd.isnull(x):
            return x
        elif x.lower().find("automatik") != -1:
            return "7-Gang Doppelkupplungsgetriebe (PDK)"
        elif x.lower().find("schaltgetriebe") != -1:
            return "6-Gang-GT-Sportschaltgetriebe"
        else:
            return x
    
    def amend_getriebe_col_stg_2_porsche_992_gt3(self, x):
        """
        A function to amend the `getriebe` column for Porsche 992 GT3 (stg 2)
        """
        if x["marke"] == "Porsche" and x["modell"] == "992" and x["leistung"] == 510\
        and (x["variante"] == "GT3" or x["variante"] == "GT3 mit Touring-Paket")\
        and (x["titel"].lower().find("pdk") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("pdk") != -1) and (x["getriebe"] == "" or x["getriebe"] is None):
            return "7-Gang Doppelkupplungsgetriebe (PDK)"
        elif x["marke"] == "Porsche" and x["modell"] == "992" and x["leistung"] == 510\
        and (x["variante"] == "GT3" or x["variante"] == "GT3 mit Touring-Paket")\
        and (x["titel"].lower().find("6-gang") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("6-gang") != -1) and (x["getriebe"] == "" or x["getriebe"] is None):
            return "6-Gang-GT-Sportschaltgetriebe"
        else:
            return x["getriebe"]

    def amend_getriebe_col_stg_3_porsche_992_gt3(self, x):
        """
        A function to amend the `getriebe` column for Porsche 992 GT3 (stg 3)
        """
        if x["marke"] == "Porsche" and x["modell"] == "992" and x["leistung"] == 510\
        and (x["variante"] == "GT3" or x["variante"] == "GT3 mit Touring-Paket")\
        and (x["getriebe"] == "" or x["getriebe"] is None):
            return "7-Gang Doppelkupplungsgetriebe (PDK)"
        else:
            return x["getriebe"]
    
    def amend_getriebe_col_stg_1_lamborghini_urus(self, x):
        """
        A function to amend the `getriebe` column for Lamborghini Urus (stg 1)
        """
        if x == "" or x is None or pd.isnull(x):
            return "8-Gang-Automatik-Getriebe"
        elif x.lower().find("automatik") != -1 or x.lower().find("halbautomatik") != -1 or x.lower().find("schaltgetriebe") != -1:
            return "8-Gang-Automatik-Getriebe"
        else:
            return x
    
    def amend_getriebe_col_stg_1_aston_martin_dbx(self, x):
        """
        A function to amend the `getriebe` column for Aston Martin DBX (stg 1)
        """
        if x == "" or x is None or pd.isnull(x):
            return "9-Gang-Automatik-Getriebe"
        elif x.lower().find("automatik") != -1:
            return "9-Gang-Automatik-Getriebe"
        else:
            return x
    
    def amend_getriebe_col_mercedes_benz_g_63_amg(self, x):
        """
        A function to amend the `getriebe` column for Mercedes-Benz G 63 AMG
        """
        if x == "" or x is None or pd.isnull(x) or x.lower().find("automatik") != -1 or x.lower().find("schaltgetriebe") != -1\
        or x.lower().find("halbautomatik") != -1:
            return "9-Gang-AMG-Speedshift"
        else:
            return x
    
    def amend_getriebe_col_mercedes_benz_sls_amg(self, x):
        """
        A function to amend the `getriebe` column for Mercedes-Benz SLS AMG
        """
        if x == "" or x is None or pd.isnull(x) or x.lower().find("automatik") != -1 or x.lower().find("schaltgetriebe") != -1\
        or x.lower().find("halbautomatik") != -1:
            return "7-Gang-Doppelkupplungs-Getriebe"
        else:
            return x
    
    def amend_getriebe_col_mclaren(self, x):
        """
        A function to amend the `getriebe` column for McLaren
        """
        if x == "" or x is None or pd.isnull(x) or x.lower().find("automatik") != -1 or x.lower().find("schaltgetriebe") != -1\
        or x.lower().find("halbautomatik") != -1:
            return "7-Gang-Doppelkupplungs-Getriebe"
        else:
            return x
    
    def amend_getriebe_col_maserati(self, x):
        """
        A function to amend the `getriebe` column for McLaren
        """
        if x == "" or x is None or pd.isnull(x) or x.lower().find("automatik") != -1 or x.lower().find("schaltgetriebe") != -1\
        or x.lower().find("halbautomatik") != -1:
            return "8-Gang-Doppelkupplungs-Getriebe"
        else:
            return x
    
    def amend_getriebe_col_ferrari_sf90(self, x):
        """
        A function to amend the `getriebe` column for Ferrari SF90
        """
        return self.amend_getriebe_col_maserati(x)
    
    def amend_getriebe_col_ferrari_812(self, x):
        """
        A function to amend the `getriebe` column for Ferrari 812
        """
        if x == "" or x is None or pd.isnull(x) or x.lower().find("automatik") != -1 or x.lower().find("schaltgetriebe") != -1\
        or x.lower().find("halbautomatik") != -1:
            return "7-Gang-Doppelkupplungs-Getriebe"
        else:
            return x
        
    def amend_getriebe_col_ferrari_f12(self, x):
        """
        A function to amend the `getriebe` column for Ferrari F12
        """
        return self.amend_getriebe_col_ferrari_812(x)
    
    ###------------------------------###------------------------------###

    ## Marke column helper functions
    def amend_marke_col_porsche_992_gt3(self, x, y, replacement_word):
        """
        A function to amend the `marke` column for Porsche 992 GT3
        """
        if x.lower().find(y.lower()) != -1:
            return "Porsche | " + replacement_word
        
    def amend_marke_col_lamborghini_urus(self, x, y, replacement_word):
        """
        A function to amend the `marke` column for Lamborghini Urus
        """
        if y == "Abt":
            if x["titel"].lower().find(y.lower()) != -1:
                return "Lamborghini | " + replacement_word
            else:
                return x["marke"]
        else:
            if x["titel"].lower().find(y.lower()) != -1 or x["fahrzeugbeschreibung_mod"].lower().find(y.lower()) != -1:
                return "Lamborghini | " + replacement_word
            else:
                return x["marke"]
    
    def amend_marke_col_aston_martin_dbx(self, x, y, replacement_word):
        """
        A function to amend the `marke` column for Aston Martin DBX
        """
        if x["titel"].lower().find(y.lower()) != -1 or x["fahrzeugbeschreibung_mod"].lower().find(y.lower()) != -1:
            return "Aston Martin | " + replacement_word
        else:
            return x["marke"]
    
    def amend_marke_col_various_brands(self, x, y, replacement_word, marke_var):
        """
        A function to amend the `marke` column for various brands
        """
        if x["titel"].lower().find(y.lower()) != -1:
            return f"{marke_var} | " + replacement_word
        else:
            return x["marke"]
        
    ###------------------------------###------------------------------###
    
    def amend_modell_and_variante_cols_stg_1_mercedes_benz_g_63_amg(self, x, col_to_amend):
        """
        A function to amend the `modell` or `variante` columns for Mercedes-Benz G 63 AMG (stg 1)
        """
        if x["marke"] == "Mercedes-Benz" and (
            x["titel"].lower().find("4x4") != "-1" or
            x["titel"].lower().find("4 x 4") != "-1" or
            x["titel"].lower().find("4x4²") != "-1" or
            x["titel"].lower().find("4²") != "-1" or
            x["titel"].lower().find("4 x4") != "-1" or
            x["titel"].lower().find("4x 4") != "-1"
        ):
            return "G 63 4x4"
        else:
            return x[col_to_amend]
    
    def amend_modell_col_stg_2_mercedes_benz_g_63_amg(self, x):
        """
        A function to amend the `modell` column for Mercedes-Benz G 63 AMG (stg 2)
        """
        if x["marke"] == "Mercedes-Benz" and x["modell"] == "G 63 AMG" and x["leistung"] == 585:
            return "G 63 (W464)"
        else:
            return x["modell"]
    
    ###------------------------------###------------------------------###

    ## Variante column helper functions
    def amend_variante_col_porsche_992_gt3(self, x):
        """
        A function to amend the `variante` column for Porsche 992 GT3
        """
        if x["marke"] == "Porsche" and x["modell"] == "992" and x["leistung"] == 510 and (x["titel"].lower().find("touring") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("touring") != -1):
            return "GT3 mit Touring-Paket"
        elif x["marke"] == "Porsche" and x["modell"] == "992" and x["leistung"] == 510 and (x["titel"].lower().find("touring") == -1 and x["fahrzeugbeschreibung_mod"].lower().find("touring") == -1):
            return "GT3"
        else:
            return x["variante"]
    
    def amend_variante_col_lamborghini_urus_stg_1(self, x):
        """
        A function to amend the `variante` column for Lamborghini Urus (stg 1)
        """
        if x["marke"] == "Lamborghini" and x["modell"] == "Urus" and (x["titel"].lower().find("performante") != -1):
            return "Urus Performante"
        else:
            return x["variante"]

    def amend_variante_col_lamborghini_urus_stg_2(self, x):
        """
        A function to amend the `variante` column for Lamborghini Urus (stg 2)
        """
        if x["marke"] == "Lamborghini" and x["modell"] == "Urus" and x["variante"] == "Urus Performante":
            return "Urus S"
        else:
            return x["variante"]

    def amend_variante_col_lamborghini_urus_stg_3(self, x):
        """
        A function to amend the `variante` column for Lamborghini Urus (stg 3)
        """
        if x["marke"] == "Lamborghini" and x["modell"] == "Urus" and (x["variante"] == "" or x["variante"] is None) and x["leistung"] == 666:
            return "Urus S"
        else:
            return x["variante"]

    def amend_variante_col_lamborghini_urus_stg_4(self, x):
        """
        A function to amend the `variante` column for Lamborghini Urus (stg 4)
        """
        if x["marke"] == "Lamborghini" and x["modell"] == "Urus" and (x["variante"] == "" or x["variante"] is None) and (x["leistung"] == 650 or x["leistung"] == 649 or x["leistung"] == 662):
            return "Urus"
        else:
            return x["variante"]
        
    def amend_variante_col_lamborghini_urus_stg_5(self, x):
        """
        A function to amend the `variante` column for Lamborghini Urus (stg 5)
        """
        if x["ausstattung"] == "Pearl Capsule" and\
        (x["variante"] == "" or x["variante"] is None or x["leistung"] == "" or x["leistung"] is None):
            return "Urus"
        else:
            return x["variante"]
        

    def amend_variante_col_aston_martin_dbx_stg_1(self, x):
        """
        A function to amend the `variante` column for Aston Martin DBX (stg 1)
        """
        if x["marke"] == "Aston Martin" and x["modell"] == "DBX" and (x["titel"].lower().find("707") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("707") != -1):
            return "DBX707"
        else:
            return x["variante"]

    def amend_variante_col_aston_martin_dbx_stg_2(self, x):
        """
        A function to amend the `variante` column for Aston Martin DBX (stg 2)
        """
        if x["marke"] == "Aston Martin" and x["modell"] == "DBX" and x["variante"] != "DBX707":
            return "DBX V8"
        else:
            return x["variante"]
    
    def amend_variante_col_mercedes_benz_g_63_amg(self, x):
        """
        A function to amend the `modell` column for Mercedes-Benz G 63 AMG (stg 2)
        """
        if x["modell"] == "G 63 (W464)" and x["leistung"] == 585:
            return "G 63"
        else:
            return x["variante"]
        
    def amend_variante_col_mercedes_benz_sls_amg_stg_1(self, x):
        """
        A function to amend the `variante` column for Mercedes-Benz SLS AMG (stg 1)
        """
        if x["marke"] == "Mercedes-Benz" and x["modell"] == "SLS AMG" and (x["leistung"] >= 625 and x["leistung"] <= 640)\
        and x["form"] == "Coupe" and x["titel"].lower().find("black series") != -1:
            return "SLS AMG Black Series"
        else:
            return x["variante"]
    
    def amend_variante_col_mercedes_benz_sls_amg_stg_2(self, x):
        """
        A function to amend the `variante` column for Mercedes-Benz SLS AMG (stg 2)
        """
        if x["marke"] == "Mercedes-Benz" and x["modell"] == "SLS AMG" and x["form"] == "Coupe"\
        and x["titel"].lower().find("final edition") != -1:
            return "SLS AMG GT Final Edition"
        elif x["marke"] == "Mercedes-Benz" and x["modell"] == "SLS AMG" and x["form"] == "Roadster"\
        and x["titel"].lower().find("final edition") != -1:
            return "SLS AMG GT Final Edition Roadster"
        else:
            return x["variante"]
    
    def amend_variante_col_mercedes_benz_sls_amg_stg_3(self, x):
        """
        A function to amend the `variante` column for Mercedes-Benz SLS AMG (stg 3)
        """
        if x["marke"] == "Mercedes-Benz" and x["modell"] == "SLS AMG" and x["form"] == "Coupe"\
        and x["titel"].find("GT") != -1 and x["leistung"] == 591:
            return "SLS AMG GT"
        elif x["marke"] == "Mercedes-Benz" and x["modell"] == "SLS AMG" and x["form"] == "Roadster"\
        and x["titel"].find("GT") != -1 and x["leistung"] == 591:
            return "SLS AMG GT Roadster"
        else:
            return x["variante"]
    
    def amend_variante_col_mercedes_benz_sls_amg_stg_4(self, x):
        """
        A function to amend the `variante` column for Mercedes-Benz SLS AMG (stg 4)
        """
        if x["marke"] == "Mercedes-Benz" and x["modell"] == "SLS AMG" and x["form"] == "Coupe"\
        and x["leistung"] == 571:
            return "SLS AMG"
        elif x["marke"] == "Mercedes-Benz" and x["modell"] == "SLS AMG" and x["form"] == "Roadster"\
        and x["leistung"] == 571:
            return "SLS AMG Roadster"
        else:
            return x["variante"]
    
    def amend_variante_col_mclaren_765lt(self, x):
        """
        A function to amend the `modell` column for McLaren 765LT
        """
        if x["marke"] == "McLaren" and x["modell"] == "765LT" and x["form"] == "Coupe":
            return "765LT"
        elif x["marke"] == "McLaren" and x["modell"] == "765LT" and x["form"] == "Spider":
            return "765LT Spider"
        else:
            return x["variante"]
    
    def amend_variante_col_mclaren_720s(self, x):
        """
        A function to amend the `modell` column for McLaren 765LT
        """
        if x["marke"] == "McLaren" and x["modell"] == "720S" and x["form"] == "Coupe" and x["leistung"] == 720:
            return "720S"
        elif x["marke"] == "McLaren" and x["modell"] == "720S" and x["form"] == "Spider" and x["leistung"] == 720:
            return "720S Spider"
        else:
            return x["variante"]
    
    def amend_variante_col_maserati_mc20(self, x):
        """
        A function to amend the `modell` column for McLaren 765LT
        """
        if x["marke"] == "Maserati" and x["modell"] == "MC20" and x["form"] == "Spider" and x["titel"].lower().find("cielo") != -1:
            return "MC20 Cielo"
        elif x["marke"] == "Maserati" and x["modell"] == "MC20" and x["form"] == "Coupe":
            return "MC20"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_sf90_stg_1(self, x):
        """
        A function to amend the `variante` column for Ferrari SF90 (stg 1)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and x["titel"].lower().find("xx") != -1 and x["form"] == "Coupe":
            return "SF90 XX Stradale"
        elif x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and x["titel"].lower().find("xx") != -1 and x["form"] == "Spider":
            return "SF90 XX Spider"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_sf90_stg_2(self, x):
        """
        A function to amend the `variante` column for Ferrari SF90 (stg 2)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and x["variante"] == "" or x["variante"] is None or pd.isnull(x["variante"])\
        and (x["titel"].lower().find("spider") != -1 or x["titel"].lower().find("spyder") != -1):
            return "SF90 Spider"
        elif x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and x["variante"] == "" or x["variante"] is None or pd.isnull(x["variante"])\
        and x["titel"].lower().find("stradale") != -1:
            return "SF90 Stradale"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_sf90_stg_3(self, x):
        """
        A function to amend the `variante` column for Ferrari SF90 (stg 3)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and x["variante"] == "" or x["variante"] is None or pd.isnull(x["variante"])\
        and x["form"] == "Coupe":
            return "SF90 Stradale"
        if x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and x["variante"] == "" or x["variante"] is None or pd.isnull(x["variante"])\
        and x["form"] == "Spider":
            return "SF90 Spider"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_812_stg_1(self, x):
        """
        A function to amend the `variante` column for Ferrari 812 (stg 1)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "812"\
        and (
            x["titel"].lower().find("aperta") != -1 or\
            x["titel"].lower().find("competizione a") != -1 or\
            x["fahrzeugbeschreibung_mod"].lower().find("aperta") != -1 or\
            x["fahrzeugbeschreibung_mod"].lower().find("competizione a") != -1
        ):
            return "812 Competizione A"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_812_stg_2(self, x):
        """
        A function to amend the `variante` column for Ferrari 812 (stg 2)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "812"\
        and (
            x["titel"].lower().find("competi") != -1 or\
            x["fahrzeugbeschreibung_mod"].lower().find("competi") != -1
        ):
            return "812 Competizione"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_812_stg_3(self, x):
        """
        A function to amend the `variante` column for Ferrari 812 (stg 3)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "812"\
        and (
            x["titel"].lower().find("gts") != -1
        ):
            return "812 GTS"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_812_stg_4(self, x):
        """
        A function to amend the `variante` column for Ferrari 812 (stg 4)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "812"\
        and (
            x["titel"].lower().find("superfast") != -1
        ):
            return "812 Superfast"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_812_stg_5(self, x):
        """
        A function to amend the `variante` column for Ferrari 812 (stg 5)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "812"\
        and x["variante"] == "" or x["variante"] is None or pd.isnull(x["variante"])\
        and (x["leistung"] >= 780 and x["leistung"] <= 820):
            if x["form"] == "Coupe":
                return "812 Superfast"
            elif x["form"] == "Cabrio":
                return "812 GTS"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_f12_stg_1(self, x):
        """
        A function to amend the `variante` column for Ferrari F12 (stg 1)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "F12"\
        and (x["leistung"] >= 770 and x["leistung"] <= 800)\
        and (
            x["titel"].lower().find("tdf") != -1
        ):
            return "F12 TDF"
        else:
            return x["variante"]
    
    def amend_variante_col_ferrari_f12_stg_2(self, x):
        """
        A function to amend the `variante` column for Ferrari F12 (stg 2)
        """
        if x["marke"] == "Ferrari" and x["modell"] == "F12"\
        and (x["leistung"] >= 730 and x["leistung"] <= 750)\
        and (
            x["titel"].lower().find("tdf") != -1
        ):
            return "F12 Berlinetta"
        else:
            return x["variante"]
    
    ###------------------------------###------------------------------###
    
    ## Leistung column helper functions
    def amend_leistung_col_lamborghini_urus_stg_1(self, x):
        """
        A function to amend the `leistung` column for Lamborghini Urus (stg 1)
        """
        if x["variante"] == "Urus Performante":
            return 666
        else:
            return x["leistung"]

    def amend_leistung_col_lamborghini_urus_stg_2(self, x):
        """
        A function to amend the `leistung` column for Lamborghini Urus (stg 2)
        """
        if x["variante"] == "Urus":
            return 650
        else:
            return x["leistung"]
        
    def amend_leistung_col_lamborghini_urus_stg_3(self, x):
        """
        A function to amend the `leistung` column for Lamborghini Urus (stg 3)
        """
        if x["ausstattung"] == "Pearl Capsule" and\
        (x["variante"] == "" or x["variante"] is None or x["leistung"] == "" or x["leistung"] is None):
            return 650
        else:
            return x["leistung"]
    
    def amend_leistung_col_aston_martin_dbx_stg_1(self, x):
        """
        A function to amend the `leistung` column for Aston Martin DBX (stg 1)
        """
        if x["variante"] == "DBX707" or x["variante"] == "" or x["variante"] is None:
            return 707
        else:
            return x["leistung"]

    def amend_leistung_col_aston_martin_dbx_stg_2(self, x):
        """
        A function to amend the `leistung` column for Aston Martin DBX (stg 2)
        """
        if x["variante"] == "DBX V8":
            return 550
        else:
            return x["leistung"]
    
    def amend_leistung_col_mercedes_benz_sls_amg_stg_1(self, x):
        """
        A function to amend the `leistung` column for Mercedes-Benz SLS AMG (stg 1)
        """
        if x["variante"] == "SLS AMG Black Series":
            return 631
        else:
            return x["leistung"]
    
    def amend_leistung_col_mercedes_benz_sls_amg_stg_2(self, x):
        """
        A function to amend the `leistung` column for Mercedes-Benz SLS AMG (stg 2)
        """
        if x["variante"] == "SLS AMG GT Final Edition" or x["variante"] == "SLS AMG GT Final Edition Roadster":
            return 591
        else:
            return x["leistung"]
    
    def amend_leistung_col_mercedes_benz_sls_amg_stg_3(self, x):
        """
        A function to amend the `leistung` column for Mercedes-Benz SLS AMG (stg 3)
        """
        if x["variante"] == "SLS AMG GT" or x["variante"] == "SLS AMG GT Roadster":
            return 591
        else:
            return x["leistung"]
    
    def amend_leistung_col_mclaren_720s(self, x):
        """
        A function to amend the `leistung` column for McLaren 720S
        """
        if x["marke"] == "McLaren" and x["modell"] == "720S"\
        and ((x["leistung"] >= 710 and x["leistung"] <= 730) or x["leistung"] == "" or x["leistung"] is None or pd.isnull(x["leistung"])):
            return 720
        else:
            return x["leistung"]
    
    def amend_leistung_col_maserati_mc20(self, x):
        """
        A function to amend the `leistung` column for Maserati MC20
        """
        if x["variante"] == "MC20 Cielo" or x["variante"] == "MC20":
            return 630
        else:
            return x["leistung"]
    
    def amend_leistung_col_ferrari_sf90_stg_1(self, x):
        """
        A function to amend the `leistung` column for Ferrari SF90 (stg 1)
        """
        if x["variante"] == "SF90 XX Stradale" or x["variante"] == "SF90 XX Spider":
            return 1030
        else:
            return x["leistung"]
    
    def amend_leistung_col_ferrari_sf90_stg_2(self, x):
        """
        A function to amend the `leistung` column for Ferrari SF90 (stg 2)
        """
        if x["variante"] == "SF90 Stradale" or x["variante"] == "SF90 Spider":
            return 1000
        else:
            return x["leistung"]
    
    def amend_leistung_col_ferrari_812(self, x):
        """
        A function to amend the `leistung` column for Ferrari 812 (stg 1)
        """
        if x["variante"] == "812 Competizione A" or x["variante"] == "812 Competizione":
            return 830
        elif x["variante"] == "812 GTS" or x["variante"] == "812 Superfast":
            return 800
        else:
            return x["leistung"]
    
    def amend_leistung_col_ferrari_f12(self, x):
        """
        A function to amend the `leistung` column for Ferrari F12 (stg 1)
        """
        if x["variante"] == "F12 TDF":
            return 780
        elif x["variante"] == "F12 Berlinetta":
            return 740
        else:
            return x["leistung"]

    ###------------------------------###------------------------------###

    ## Ausstattung column helper functions
    def add_ausstattung_col_porsche_992_gt3(self, x):
        """
        A function to add the `Ausstattung` column for Porsche 992 GT3
        """
        if x["marke"] == "Porsche" and x["modell"] == "992" and (x["variante"] == "GT3" or x["variante"] == "GT3 mit Touring-Paket")\
        and x["form"] == "Coupé" and x["fahrzeugzustand"].lower() == "unfallfrei" and x["leistung"] == 510\
        and (
            ("pts" in x["titel"].lower() and "pccb" in x["titel"].lower()) or\
            ("pts" in x["fahrzeugbeschreibung_mod"].lower() and "pccb" in x["fahrzeugbeschreibung_mod"].lower())
        ):
            return "PTS + PCCB"
        elif x["marke"] == "Porsche" and x["modell"] == "992" and (x["variante"] == "GT3" or x["variante"] == "GT3 mit Touring-Paket")\
        and x["form"] == "Coupé" and x["fahrzeugzustand"].lower() == "unfallfrei" and x["leistung"] == 510\
        and (x["titel"].lower().find("pts") != -1):
            return "PTS"
        elif x["marke"] == "Porsche" and x["modell"] == "992" and (x["variante"] == "GT3" or x["variante"] == "GT3 mit Touring-Paket")\
        and x["form"] == "Coupé" and x["fahrzeugzustand"] == "Unfallfrei" and x["leistung"] == 510\
        and (x["titel"].lower().find("pccb") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("pccb") != -1):
            return "PCCB"
        else:
            return None
    
    def add_ausstattung_col_lamborghini_urus(self, x):
        """
        A function to add the `Ausstattung` column for Lamborghini Urus (stg 1)
        """
        if x["marke"] == "Lamborghini" and x["modell"] == "Urus"\
        and (x["titel"].lower().find("capsu") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("capsu") != -1):
            return "Pearl Capsule"
        else:
            return None
    
    def add_ausstattung_col_aston_martin_dbx(self, x):
        """
        A function to add the `Ausstattung` column for Aston Martin DBX
        """
        if x["marke"] == "Aston Martin" and x["modell"] == "DBX"\
        and x["variante"] == "DBX V8" and x["form"] == "SUV" and x["fahrzeugzustand"].lower() == "unfallfrei"\
        and x["leistung"] == 550 and (x["titel"].lower().find("1913") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("1913") != -1):
            return "1913 Edition"
        else:
            return None
    
    def add_ausstattung_col_mercedes_benz_g_63_amg(self, x):
        """
        A function to add the `Ausstattung` column for Mercedes-Benz G 63 AMG
        """
        if x["marke"] == "Mercedes-Benz" and x["modell"] == "G 63 (W464)"\
        and x["leistung"] == 585:
            if x["titel"].lower().find("grand") != -1:
                return "Grand Edition"
            elif x["titel"].lower().find("55") != -1:
                return "Edition 55"
            elif (x["titel"].lower().find("edition 1") != -1 or x["titel"].lower().find("edition one") != -1):
                return "Edition 1"
            elif x["titel"].lower().find("stronger than time") != -1:
                return "Stronger Than Time Edition"
            else:
                return None
        else:
            return None
    
    def add_ausstattung_col_mclaren_765lt(self, x):
        """
        A function to add the `Ausstattung` column for McLaren 765LT
        """
        if x["marke"] == "McLaren" and x["modell"] == "765LT"\
        and x["titel"].lower().find("mso") != -1:
            return "MSO"
        else:
            return None
    
    def add_ausstattung_col_mclaren_720s(self, x):
        """
        A function to add the `Ausstattung` column for McLaren 720S
        """
        if x["marke"] == "McLaren" and x["modell"] == "720S"\
        and x["titel"].lower().find("apex") != -1:
            return "MSO Apex Collection"
        elif x["marke"] == "McLaren" and x["modell"] == "720S"\
        and x["titel"].lower().find("performance") != -1:
            return "Performance Pack"
        else:
            return None
    
    def add_ausstattung_col_ferrari_sf90(self, x):
        """
        A function to add the `Ausstattung` column for Ferrari SF90
        """
        if x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and (
            (x["titel"].lower().find("atelier") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("atelier") != -1)\
            and (x["titel"].lower().find("assetto") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("assetto") != -1)
        ):
            return "Assetto Fiorano | Atelier Car"
        elif x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and (x["titel"].lower().find("tailor") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("tailor") != -1):
            return "Tailor Made"
        elif x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and (x["titel"].lower().find("atelier") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("atelier") != -1):
            return "Atelier Car"
        elif x["marke"] == "Ferrari" and x["modell"] == "SF90"\
        and (x["titel"].lower().find("assetto") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("assetto") != -1):
            return "Assetto Fiorano"
        else:
            return None
    
    def add_ausstattung_col_ferrari_812(self, x):
        """
        A function to add the `Ausstattung` column for Ferrari 812
        """
        if x["marke"] == "Ferrari" and x["modell"] == "812"\
        and (x["titel"].lower().find("atelier") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("atelier") != -1):
            return "Atelier Car"
        elif x["marke"] == "Ferrari" and x["modell"] == "812"\
        and (x["titel"].lower().find("tailor") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("tailor") != -1):
            return "Tailor Made"
        else:
            return None
    
    def add_ausstattung_col_ferrari_f12(self, x):
        """
        A function to add the `Ausstattung` column for Ferrari 812
        """
        return self.add_ausstattung_col_ferrari_812(x)

class CleaningFunctions(HelperFunctions):  
    ### Porsche
    ## 992 GT3
    def clean_porsche_992_gt3(self, df_specific_brand):
        """
        A function to clean the data of Porsche 992 GT3
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        # Add fahrzeugbeschreibung_mod column to replace None values in that column with an empty string
        df_clean_1["fahrzeugbeschreibung_mod"] = df_clean_1["fahrzeugbeschreibung"].apply(lambda x: "" if x is None else x)

        # Filter for GT3 or GT 3
        df_clean_2 = df_clean_1.loc[
            (df_clean_1["titel"].str.lower().str.contains("gt3|gt 3")) |\
            (df_clean_1["fahrzeugbeschreibung_mod"].str.lower().str.contains("gt3|gt 3"))
        ].reset_index(drop=True)
        
        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Sportwagen/Coupe, oder Sportwagen/Coupe, Jahreswagen, oder Sportwagen/Coupe, Neufahrzeug, oder Sportwagen/Coupe, Tageszulassung, oder
        # Sportwagen/Coupe, Vorführfahrzeug abgebildet wird, dann auf „Coupé“ ändern!
        
        # Make a copy of df_clean_2
        df_clean_3 = df_clean_2.copy()

        # Remove the spaces from the form column and convert to lower case
        df_clean_3["form_mod"] = df_clean_3["form"].apply(lambda x: x.replace(" ", "").lower())

        # Apply the function amend_form_col on form using the apply method
        df_clean_3["form"] = df_clean_3["form_mod"].apply(self.amend_form_col_porsche_992_gt3)

        # Drop the form_mod column
        df_clean_3 = df_clean_3.drop("form_mod", axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn Unfallfrei, Nicht fahrtauglich, oder (Leere) abgebildet wird, dann auf “Unfallfrei“ ändern
        
        # Make a copy of df_clean_3
        df_clean_4 = df_clean_3.copy()

        df_clean_4["fahrzeugzustand"] = df_clean_4["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)

        ###------------------------------###------------------------------###

        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_5 = df_clean_4.copy()
        df_clean_5["kilometer"] = df_clean_5["kilometer"].apply(self.amend_kilometer_col)
        
        ###------------------------------###------------------------------###

        ## Amend the `Getriebe` column
        # Spalte H = getriebe = Wenn getriebe Automatik, dann ändere auf „7-Gang Doppelkupplungsgetriebe (PDK)“
        # Spalte H = getriebe = Wenn getriebe Schaltgetriebe, dann ändere auf „6-Gang-GT-Sportschaltgetriebe“
        df_clean_6 = df_clean_5.copy()
        df_clean_6["getriebe"] = df_clean_6["getriebe"].apply(self.amend_getriebe_col_stg_1_porsche_992_gt3)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte D -> A = titel = Wenn im titel „Techart“ steht, dann ändere marke (Spalte A) auf “Porsche/Techart“
        # Spalte D -> A = titel = Wenn im titel „Manthey“ steht, dann ändere marke (Spalte A) auf “Porsche/Manthey Performance“
        # Spalte D -> A = titel = Wenn im titel „Cup“ steht, dann ändere marke (Spalte A) auf “Porsche/Racing“
        df_clean_7 = df_clean_6.copy()
        df_clean_7["marke"] = df_clean_7.apply(lambda x: self.amend_marke_col_porsche_992_gt3(x["titel"], "Techart", "Techart") if x["titel"].lower().find("techart") != -1 else x["marke"], axis=1)
        df_clean_7["marke"] = df_clean_7.apply(lambda x: self.amend_marke_col_porsche_992_gt3(x["titel"], "Manthey", "Manthey") if x["titel"].lower().find("manthey") != -1 else x["marke"], axis=1)
        df_clean_7["marke"] = df_clean_7.apply(lambda x: self.amend_marke_col_porsche_992_gt3(x["titel"], "Cup", "Racing") if x["titel"].lower().find("cup") != -1 else x["marke"], axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `variante` column
        # Rule 1: Spate A|B|D|G|O verändern Spalte C = variante = Wenn marke Porsche, und modell 992, und leistung 510, und entweder im titel Touring oder in fahrzeugbeschreibung Touring steht, dann ändere in Spalte C auf “GT3 mit Touring-Paket“
        # Rule 2: Spate A|B|D|G|O verändern Spalte C = variante = Wenn marke Porsche, und Modell 992, und leistung 510, und Spalte C nicht GT3 mit Touring-Paket steht, dann ändere auf “GT3“.

        # Rule 1: Spate H = getriebe = Wenn marke Porsche, und Modell 992, und leistung 510, und Variante GT3 oder GT3 mit Touring-Paket, und entweder im titel PDK oder im fahrzeugbeschreibung PDK steht, und getriebe auf (Leere), dann ändere auf “7-Gang Doppelkupplungsgetriebe (PDK)“
        # Rule 2: Spate H = getriebe = Wenn marke Porsche, und Modell 992, und leistung 510, und Variante GT3 oder GT3 mit Touring-Paket, und entweder im titel 6-Gang oder im fahrzeugbeschreibung 6-Gang steht, und getriebe auf (Leere), dann ändere auf "6-Gang-GT-Sportschaltgetriebe"
        
        # Rule 3: Spate H = getriebe = Wenn marke Porsche, und Modell 992, und leistung 510, und Variante GT3 oder GT3 mit Touring-Paket, und getriebe auf (Leere), dann ändere auf “7-Gang Doppelkupplungsgetriebe (PDK)“.“.
        df_clean_8 = df_clean_7.copy()
        df_clean_8["variante"] = df_clean_8.apply(
            lambda x: self.amend_variante_col_porsche_992_gt3(x), axis=1
        )

        # Getriebe Stg 2
        df_clean_8["getriebe"] = df_clean_8.apply(lambda x: self.amend_getriebe_col_stg_2_porsche_992_gt3(x), axis=1)

        # Getriebe Stg 3
        df_clean_8["getriebe"] = df_clean_8.apply(lambda x: self.amend_getriebe_col_stg_3_porsche_992_gt3(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D = Wenn marke Porsche, und Modell 992, und Variante GT3 oder GT3 mit Touring-Paket, und form Coupe, und fahrzeugzustand unfallfrei, und Leistung 510, und im titel PTS steht, dann ändere auf „PTS“.
        # Neue Spalte D = Wenn marke Porsche, und Modell 992, und Variante GT3 oder GT3 mit Touring-Paket, und form Coupe, und fahrzeugzustand unfallfrei, und Leistung 510, und entweder im titel PCCB oder im Fahrzeugbeschreibung PCCB steht, dann ändere auf „PCCB“. (Info: Wenn das modell schon die Ausstattung PTS hat und auch PCCB, dann ändere auf „PTS | PCCB“)
        # Neue Spalte D = Wenn marke Porsche, und Modell 992, und Variante GT3 oder GT3 mit Touring-Paket, und form Coupe, und fahrzeugzustand unfallfrei, und Leistung 510, und entweder im titel Manufa oder im Fahrzeugbeschreibung Manufa steht, dann ändere auf „Exklusive Manufaktur“.
        df_clean_9 = df_clean_8.copy()
        df_clean_9["ausstattung"] = df_clean_9.apply(lambda x: self.add_ausstattung_col_porsche_992_gt3(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_9.pop("ausstattung")
        df_clean_9.insert(3, "ausstattung", austattung_col)

        # Drop the fahrzeugbeschreibung_mod column
        df_clean_9 = df_clean_9.drop("fahrzeugbeschreibung_mod", axis=1)

        return df_clean_9

    ### Lamborghini
    ## Urus
    def clean_lamborghini_urus(self, df_specific_brand):
        """
        A function to clean the data of Lamborghini Urus
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        # Add fahrzeugbeschreibung_mod column to replace None values in that column with an empty string
        df_clean_1["fahrzeugbeschreibung_mod"] = df_clean_1["fahrzeugbeschreibung"].apply(lambda x: "" if x is None else x)

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn SUV/Geländewagen/Pickup, oder SUV/Geländewagen/Pickup, Jahreswagen, oder SUV/Geländewagen/Pickup, Neufahrzeug, oder SUV/Geländewagen/Pickup, Tageszulassung, oder SUV/Geländewagen/Pickup, Vorführfahrzeug, oder Andere, oder Andere, Neufahrzeug, oder Sportwagen/Coupe, oder Sportwagen/Coupe, Neufahrzeug, oder Sportwagen/Coupe, Vorführfahrzeug abgebildet wird, dann auf „Super Sport Utility Vehicle“ ändern!
        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_lamborghini_urus)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn Unfallfrei, Nicht fahrtauglich, oder (Leere) abgebildet wird, dann auf “Unfallfrei“ ändern
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)

        ###------------------------------###------------------------------###

        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `Getriebe` col
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_stg_1_lamborghini_urus)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte D -> A = titel = Wenn entweder im titel „gepanzert“ oder fahrzeugbeschreibung „gepanzert“ steht, dann ändere marke (Spalte A) auf “Lamborghini/Trasco“. (Info: Tuning Autos werden nicht berücksichtigt)
        # Spalte D -> A = titel = Wenn entweder im titel „Mansory“oder fahrzeugbeschreibung Mansory steht, dann ändere marke (Spalte A) auf “Lamborghini/Mansory“.
        # Spalte D -> A = titel = Wenn entweder im titel „Novitec“oder fahrzeugbeschreibung Novitec steht, dann ändere marke (Spalte A) auf “Lamborghini/Novitec“.
        # Spalte D -> A = titel = Wenn entweder im titel „Keyvany“oder fahrzeugbeschreibung Keyvany steht, dann ändere marke (Spalte A) auf “Lamborghini/Keycany
        # Spalte D -> A = titel = Wenn im titel „Abt“ steht, dann ändere marke (Spalte A) auf “Lamborghini/Abt“
        df_clean_6 = df_clean_5.copy()

        lamborghini_urus_marke_dict = {
            "gepanzert": "Trasco",
            "mansory": "Mansory",
            "novitec": "Novitec",
            "keyvany": "Keyvany",
            "abt": "Abt"
        }

        for key, value in lamborghini_urus_marke_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_lamborghini_urus(x, key, value), axis=1)
    
        ###------------------------------###------------------------------###

        ## Amend the `variante` and leistung columns
        # Spalte A|B|D|G|O verändern Spalte C= variante = Wenn marke Lamborghini , und Modell Urus, und im titel Performante steht, dann ändere Variante auf Urus Performante.
        # Spalte G = Leistung = Wenn variante Urus Performante,, dann auf 666 ändern

        # Spate A|B|D|G|O verändern Spalte C= variante = Wenn marke Lamborghini , und Modell Urus, und variante (leere), und leistung 666, dann ändere Variante auf Urus S.

        # Spate A|B|D|G|O verändern Spalte C= variante = Wenn marke Lamborghini , und Modell Urus, und variante (leere), und entweder leistung 650 oder leistung 649 oder Leistung 662, dann ändere Variante auf Urus.
        # Spalte G = Leistung = Wenn variante Urus Performante,, dann auf 650 ändern

        df_clean_7 = df_clean_6.copy()
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_lamborghini_urus_stg_1(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_lamborghini_urus_stg_2(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_lamborghini_urus_stg_1(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_lamborghini_urus_stg_3(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_lamborghini_urus_stg_4(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_lamborghini_urus_stg_2(x), axis=1)
        
        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D = Wenn marke Lamborghini, und model Urus, und entweder im Titel Capsu oder fahrzeugbeschreibung Capsu steht, dann ändere ausstattung auf Pearl Capsule
        # Wenn aussattung Pearl Capsule, und entweder variante leere oder leistung leere, dann ändere Variante Urus und Leistung 650
        df_clean_8 = df_clean_7.copy()

        df_clean_8["ausstattung"] = df_clean_8.apply(lambda x: self.add_ausstattung_col_lamborghini_urus(x), axis=1)
        df_clean_8["variante"] = df_clean_8.apply(lambda x: self.amend_variante_col_lamborghini_urus_stg_5(x), axis=1)
        df_clean_8["leistung"] = df_clean_8.apply(lambda x: self.amend_leistung_col_lamborghini_urus_stg_3(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        # Drop the fahrzeugbeschreibung_mod column
        df_clean_8 = df_clean_8.drop("fahrzeugbeschreibung_mod", axis=1)

        return df_clean_8

    ### Aston Martin
    ## DBX
    def clean_aston_martin_dbx(self, df_specific_brand):
        """
        A function to clean the data of Aston Martin DBX
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        # Add fahrzeugbeschreibung_mod column to replace None values in that column with an empty string
        df_clean_1["fahrzeugbeschreibung_mod"] = df_clean_1["fahrzeugbeschreibung"].apply(lambda x: "" if x is None else x)

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn SUV/Geländewagen/Pickup, oder SUV/Geländewagen/Pickup, Jahreswagen, oder SUV/Geländewagen/Pickup, Neufahrzeug, oder SUV/Geländewagen/Pickup, Tageszulassung, oder SUV/Geländewagen/Pickup, Vorführfahrzeug, oder Andere, oder Andere, Neufahrzeug, oder Sportwagen/Coupe, oder Sportwagen/Coupe, Neufahrzeug, oder Sportwagen/Coupe, Vorführfahrzeug abgebildet wird, dann auf „SUV“ ändern!
        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_aston_martin_dbx)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn Unfallfrei, Nicht fahrtauglich, oder (Leere) abgebildet wird, dann auf “Unfallfrei“ ändern
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)

        ###------------------------------###------------------------------###

        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `Getriebe` column
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_stg_1_aston_martin_dbx)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte D -> A = titel = Wenn entweder im titel „gepanzert“ oder fahrzeugbeschreibung „gepanzert“ steht, dann ändere marke (Spalte A) auf “Aston Martin/Trasco“. (Info: Tuning Autos werden nicht berücksichtigt)
        # Spalte D -> A = titel = Wenn entweder im titel „Mansory“oder fahrzeugbeschreibung Mansory steht, dann ändere marke (Spalte A) auf “Aston Martin/Mansory“.
        df_clean_6 = df_clean_5.copy()

        aston_martin_marke_dict = {
            "gepanzert": "Trasco",
            "mansory": "Mansory"
        }

        for key, value in aston_martin_marke_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_aston_martin_dbx(x, key, value), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `variante` and `leistung` columns
        # Spate A|B|D|G|O verändern Spalte C= variante = Wenn marke Aston Martin , und Modell DBX, und entweder im titel 707 oder in fahrzeugbeschreibung 707 steht, dann ändere Variante auf DBX707.
        # Spalte G = Leistung = Wenn variante DBX707, und (leere), dann auf 707 ändern

        # Spate A|B|D|G|O verändern Spalte C= variante = Wenn marke Aston Martin , und Modell DBX, und nicht variante DBX707, dann ändere Variante auf DBX V8
        # Spalte G = Leistung = Wenn variante DBX V8, dann auf 550 ändern (Info: Händler geben immeer 549 oder 551 PS auf Mobile.de an)
        df_clean_7 = df_clean_6.copy()
        
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_aston_martin_dbx_stg_1(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_aston_martin_dbx_stg_1(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_aston_martin_dbx_stg_2(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_aston_martin_dbx_stg_2(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D = Wenn marke aston Martin, und Modell DBX, und Variante DBX V8, und form SUV und fahrzeugzustand unfallfrei, und leistung 550, und entweder im titel 1913 oder im fahrzeugbeschreibung 1913 steht, dann ändere ausstattung auf 1913 Edition
        df_clean_8 = df_clean_7.copy()

        df_clean_8["ausstattung"] = df_clean_8.apply(lambda x: self.add_ausstattung_col_aston_martin_dbx(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        # Drop the fahrzeugbeschreibung_mod column
        df_clean_8 = df_clean_8.drop("fahrzeugbeschreibung_mod", axis=1)

        return df_clean_8
    
    ### Mercedes-Benz
    ## G 63 AMG
    def clean_mercedes_benz_g_63_amg(self, df_specific_brand):
        """
        A function to clean the data of Aston Martin DBX
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn SUV / Geländewagen / Pickup, oder SUV / Geländewagen / Pickup, Tageszulassung, oder SUV / Geländewagen / Pickup, Jaheswagen, oder SUV / Geländewagen / Pickup, Neufahrzeug, oder SUV / Geländewagen / Pickup, Vorführfahrzeug, dann ändere auf "SUV"
        # Spalte E = form = Wenn Van/Minibus, oder Sportwagen/Coupe, oder Limousine, oder Andere/Neufahrzeug , dann ändere auf "SUV"
        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_mercedes_benz_g_63_amg)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Spalte H = getriebe = Wenn (Leere), oder Automatik, oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "9-Gang-AMG-Speedshift"
        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_mercedes_benz_g_63_amg)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A = marke = Wenn Spalte D Titel Brabus enthält, dann ändere auf "Merceds-Benz | Brabus"
        # Spalte A = marke = Wenn Spalte D Titel entweder "gepanzert" oder "armored" oder "Panzer" enthält, dann ändere auf "Mercedes-Benz| Trasco | Hofele | etc."
        # Spalte A = marke = Wenn Spalte D Titel Keyvany enthält, dann ändere auf "Ferrari | Keyvany"
        # Spalte A = marke = Wenn Spalte D Titel Mansory enthält, dann ändere auf "Mercedes-Benz| Mansory"
        # Spalte A = marke = Wenn Spalte D Titel entweder GCD oder German Classic Design enthält, dann ändere auf "Mercedes-Benz| GCD"
        # Spalte A = marke = Wenn Spalte D Titel Lennson enthält, dann ändere auf "Mercedes-Benz| Lennson"
        # Spalte A = marke = Wenn Spalte D Titel entweder hoefele oder hofele enthält, dann ändere auf "Mercedes-Benz| Hofele"
        # Spalte A = marke = Wenn Spalte D Titel Lumma enthält, dann ändere auf "Mercedes-Benz| Lumma Design"
        # Spalte A = marke = Wenn Spalte D Titel Schawe enthält, dann ändere auf "Mercedes-Benz| Schawe Manufactur"
        df_clean_6 = df_clean_5.copy()

        mercedes_benz_g_63_amg_marke_dict = {
            "brabus": "Brabus",
            "gepanzert": "Trasco | Hofele | etc.",
            "armored": "Trasco | Hofele | etc.",
            "panzer": "Trasco | Hofele | etc.",
            "keyvany": "Keyvany",
            "mansory": "Mansory",
            "gcd": "GCD",
            "german classic design": "GCD",
            "lennson": "Lennson",
            "hoefele": "Hofele",
            "hofele": "Hofele",
            "lumma": "Lumma Design",
            "schawe": "Schawe Manufactur"
        }

        for key, value in mercedes_benz_g_63_amg_marke_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "Mercedes-Benz"), axis=1)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `Modell` column
        # Spalte B & C = Modell = Wenn Marke Mercedes-Benz, und im Titel entweder 4x4 oder 4 x 4 oder 4×4² oder 4² oder 4 x4 steht, dann ändere modell und variante auf "G63 4x4".
        # Spalte B & C = Modell = Wenn Marke Mercedes-Benz, und Modell G 63 AMG, und Leistung 585, dann ändere modell auf "G 63  (W464)".
        df_clean_7 = df_clean_6.copy()

        df_clean_7["modell"] = df_clean_7.apply(lambda x: self.amend_modell_and_variante_cols_stg_1_mercedes_benz_g_63_amg(x, "modell"), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_modell_and_variante_cols_stg_1_mercedes_benz_g_63_amg(x, "variante"), axis=1)
        df_clean_7["modell"] = df_clean_7.apply(lambda x: self.amend_modell_col_stg_2_mercedes_benz_g_63_amg(x), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `variante` column
        # Spalte C = variante = Wenn modell G 63 (W464), und leistung 585 ist, dann ändere variante auf "G 63"
        df_clean_8 = df_clean_7.copy()

        df_clean_8["variante"] = df_clean_8.apply(lambda x: self.amend_variante_col_mercedes_benz_g_63_amg(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D	Ausstattung	Wenn Marke Mercedes-Benz, und modell G 63 (W464),  und Leistung 585, und  im Titel Grand, dann ändere auf "Grand Edition".
        # Neue Spalte D	Ausstattung	Wenn Marke Mercedes-Benz, und modell G 63 (W464),  und Leistung 585, und  im Titel 55, dann ändere auf "Edition 55".
        # Neue Spalte D	Ausstattung	Wenn Marke Mercedes-Benz, und modell G 63 (W464),  und Leistung 585, und entweder im Titel Edition 1 oder Edition One, dann ändere auf "Edition 1".
        # Neue Spalte D	Ausstattung	Wenn Marke Mercedes-Benz, und modell G 63 (W464),  und Leistung 585, und im titel Stronger than time, dann ändere auf "Stronger Than Time Edition".
        df_clean_9 = df_clean_8.copy()

        df_clean_9["ausstattung"] = df_clean_9.apply(lambda x: self.add_ausstattung_col_mercedes_benz_g_63_amg(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_9.pop("ausstattung")
        df_clean_9.insert(3, "ausstattung", austattung_col)

        return df_clean_9
    
    ### Mercedes-Benz
    ## SLS AMG
    def clean_mercedes_benz_sls_amg(self, df_specific_brand):
        """
        A function to clean the data of Aston Martin DBX
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Spotwagen/Coupe, oder Sporwagen/Coupe, Tageszulassung, oder Sporwagen/Coupe, Jaheswagen, oder Sporwagen/Coupe, Neufahrzeug, oder Sporwagen/Coupe, Vorführfahrzeug, dann ändere auf "Coupé".
        # Spalte E = form = Wenn Cabrio/Roadster, oder Cabrio/Roadster, Tageszulassung, oder Cabrio/Roadster, Jaheswagen, oder Cabrio/Roadster, Neufahrzeug, oder Cabrio/Roadster, Vorführfahrzeug, dann ändere auf "Roadster".
        # Spalte E = form = Wenn im titel Roadster steht, dann ändere form auf "Roadster".
        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_mercedes_benz_sls_amg)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Spalte H = getriebe = Wenn (Leere), oder Automatik, oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "9-Gang-AMG-Speedshift"
        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_mercedes_benz_sls_amg)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A = marke = Wenn Spalte D Titel Barbus enthält, dann ändere auf "Mercedes Benz| Brabus"
        # Spalte A = marke = Wenn Spalte D Titel Umbau enthält, dann ändere auf "Mercedes Benz| Umbau"
        # Spalte A = marke = Wenn Spalte D Titel GT3 enthält, dann ändere auf "Mercedes Benz| Rennwagen"
        df_clean_6 = df_clean_5.copy()

        mercedes_benz_sls_amg_marke_dict = {
            "brabus": "Brabus",
            "umbau": "Umbau",
            "gt3": "Rennwagen"
        }

        for key, value in mercedes_benz_sls_amg_marke_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "Mercedes-Benz"), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `variante` and `leistung` columns
        # Spalte C = Variante = Wenn Marke Mercedes Benz, und modell SLS AMG, und Leistung zwischen 625 bis 640, und form Coupe, und im titel Black Series steht, dann ändere auf "SLS AMG Black Series".
        # Spalte G = Leistung = Wenn variante SLS AMG Black Series, dann ändere Leistung auf "631".
                
        # Spalte C = Variante = Wenn Marke Mercedes Benz, und modell SLS AMG, und form Coupe, und im titel Final Edition steht, dann ändere auf "SLS AMG GT Final Edition".
        # Spalte C = Variante = Wenn Marke Mercedes Benz, und modell SLS AMG, und form Roadster, und im titel Final Edition steht, dann ändere auf "SLS AMG GT Final Edition Roadster".
        # Spalte G = Leistung = Wenn entweder variante SLS AMG GT Final Edition oder SLS AMG GT Final Edition Roadster , dann ändere Leistung auf "591".

        # Spalte C = Variante = Wenn Marke Mercedes Benz, und modell SLS AMG, und form Coupe, und im titel GT steht, und leistung 591, dann ändere auf "SLS AMG GT".
        # Spalte C = Variante = Wenn Marke Mercedes Benz, und modell SLS AMG, und form Roadster, und im titel GT steht, und leistung 591, dann ändere auf "SLS AMG GT Roadster".
        # Spalte G = Leistung = Wenn entweder variante SLS AMG GT oder SLS AMG GT Roadster , dann ändere Leistung auf "591".
                
        # Spalte C = Variante = Wenn Marke Mercedes Benz, und modell SLS AMG, und form Coupe, und leistung 571, dann ändere auf "SLS AMG".
        # Spalte C = Variante = Wenn Marke Mercedes Benz, und modell SLS AMG, und form Roadster, und leistung 571, dann ändere auf "SLS AMG Roadster".
        df_clean_7 = df_clean_6.copy()

        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_mercedes_benz_sls_amg_stg_1(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_mercedes_benz_sls_amg_stg_1(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_mercedes_benz_sls_amg_stg_2(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_mercedes_benz_sls_amg_stg_2(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_mercedes_benz_sls_amg_stg_3(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_mercedes_benz_sls_amg_stg_3(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_mercedes_benz_sls_amg_stg_4(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        
        df_clean_8 = df_clean_7.copy()
        df_clean_8["ausstattung"] = None

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        return df_clean_8

    ### McLaren
    ## 765LT
    def clean_mclaren_765lt(self, df_specific_brand):
        """
        A function to clean the data of McLaren 765LT
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Spotwagen/Coupe, oder Sporwagen/Coupe, Tageszulassung, oder Sporwagen/Coupe, Jaheswagen, oder Sporwagen/Coupe, Neufahrzeug, oder Sporwagen/Coupe, Vorführfahrzeug, dann ändere auf "Coupé".
        # Spalte E = form = Wenn Cabrio/Roadster, oder Cabrio/Roadster, Tageszulassung, oder Cabrio/Roadster, Jaheswagen, oder Cabrio/Roadster, Neufahrzeug, oder Cabrio/Roadster, Vorführfahrzeug, dann ändere auf "Spider".

        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_mclaren)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Spalte H = getriebe = Wenn (Leere), oder Automatik, oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "7-Gang-Doppelkupplungs-Getriebe"
        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_mclaren)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A = marke = Wenn Spalte D Titel Novitec enthält, dann ändere auf " McLaren | Novitec"
        # Spalte A = marke = Wenn Spalte D Titel Mansory enthält, dann ändere auf " McLaren | Mansory"
        df_clean_6 = df_clean_5.copy()

        mclaren_765lt_dict = {
            "novitec": "Novitec",
            "mansory": "Mansory",
        }

        for key, value in mclaren_765lt_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "McLaren"), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `variante` column
        # Spalte C = Variante = Wenn Marke McLaren, und modell 765LT, und form Coupe, dann ändere auf "765LT"
        # Spalte C = Variante = Wenn Marke McLaren, und modell 720S, und form Spider, dann ändere auf "765LT Spider"
        df_clean_7 = df_clean_6.copy()

        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_mclaren_765lt(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D = Ausstattung = Wenn Marke McLaren, und modell 765LT, und im titel MSO steht, dann ändere auf "MSO"
        df_clean_8 = df_clean_7.copy()

        df_clean_8["ausstattung"] = df_clean_8.apply(lambda x: self.add_ausstattung_col_mclaren_765lt(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        return df_clean_8
    
    ### McLaren
    ## 720S
    def clean_mclaren_720S(self, df_specific_brand):
        """
        A function to clean the data of McLaren 720S
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Spotwagen/Coupe, oder Sporwagen/Coupe, Tageszulassung, oder Sporwagen/Coupe, Jaheswagen, oder Sporwagen/Coupe, Neufahrzeug, oder Sporwagen/Coupe, Vorführfahrzeug, dann ändere auf "Coupé".
        # Spalte E = form = Wenn Cabrio/Roadster, oder Cabrio/Roadster, Tageszulassung, oder Cabrio/Roadster, Jaheswagen, oder Cabrio/Roadster, Neufahrzeug, oder Cabrio/Roadster, Vorführfahrzeug, dann ändere auf "Spider".

        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_mclaren)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Spalte H = getriebe = Wenn (Leere), oder Automatik, oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "7-Gang-Doppelkupplungs-Getriebe"
        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_mclaren)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A = marke = Wenn Spalte D Titel Novitec enthält, dann ändere auf " McLaren | Novitec"
        # Spalte A = marke = Wenn Spalte D Titel Mansory enthält, dann ändere auf " McLaren | Mansory"
        df_clean_6 = df_clean_5.copy()

        mclaren_720s_dict = {
            "novitec": "Novitec",
            "mansory": "Mansory",
        }

        for key, value in mclaren_720s_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "McLaren"), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `leistung` column
        # Spalte G = Leistung = Wenn marke Mclaren, und modell 720S und Leistung zwischen 710 bis 730 oder leistung (Leere) ist, dann ändere leistung auf "720"
        df_clean_7 = df_clean_6.copy()

        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_mclaren_720s(x), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `variante` column
        # Spalte C = Variante = Wenn Marke McLaren, und modell 720S, und form Coupe, und leistung 720, dann ändere auf "720S".
        # Spalte C = Variante = Wenn Marke McLaren, und modell 720S, und form Spider, und leistung 720, dann ändere auf "720S Spider".
        df_clean_8 = df_clean_7.copy()

        df_clean_8["variante"] = df_clean_8.apply(lambda x: self.amend_variante_col_mclaren_720s(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D = Ausstattung = Wenn Marke McLaren, und modell 720S, und im titel Apex steht, dann ändere auf "MSO Apex Collection".
        # Neue Spalte D	= Ausstattung = Wenn Marke McLaren, und modell 720S, und im titel Performance steht, dann ändere auf "Performance Pack".
        df_clean_9 = df_clean_8.copy()

        df_clean_9["ausstattung"] = df_clean_9.apply(lambda x: self.add_ausstattung_col_mclaren_720s(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_9.pop("ausstattung")
        df_clean_9.insert(3, "ausstattung", austattung_col)

        return df_clean_9

    ### Maserati
    ## MC20
    def clean_maserati_mc20(self, df_specific_brand):
        """
        A function to clean the data of Maserati MC20
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Spotwagen/Coupe, oder Sporwagen/Coupe, Tageszulassung, oder Sporwagen/Coupe, Jaheswagen, oder Sporwagen/Coupe, Neufahrzeug, oder Sporwagen/Coupe, Vorführfahrzeug, dann ändere auf "Coupé".
        # Spalte E = form = Wenn Cabrio/Roadster, oder Cabrio/Roadster, Tageszulassung, oder Cabrio/Roadster, Jaheswagen, oder Cabrio/Roadster, Neufahrzeug, oder Cabrio/Roadster, Vorführfahrzeug, dann ändere auf "Spider".

        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_maserati)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Spalte H = getriebe = Wenn (Leere), oder Automatik, oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "7-Gang-Doppelkupplungs-Getriebe"
        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_maserati)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A	marke	Wenn Spalte D Titel Novitec enthält, dann ändere auf "Maserati | Novitec"
        df_clean_6 = df_clean_5.copy()

        maserati_mc20_dict = {
            "novitec": "Novitec"
        }

        for key, value in maserati_mc20_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "Maserati"), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `variante` column
        # Spalte C = Variante = Wenn Marke Maserati, und modell MC20, und form Spider, und titel Cielo, dann ändere auf "MC20 Cielo".
        # Spalte C = Variante = Wenn Marke Maserati, und modell MC20, und form Coupe, dann ändere auf "MC20".
        df_clean_7 = df_clean_6.copy()

        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_maserati_mc20(x), axis=1)

        ###------------------------------###------------------------------###

        ## Amend the `leistung` column
        # Spate G = Leistung = Wenn entweder variante MC20 Cielo oder MC20 , dann ändere Leistung auf "630".
        df_clean_8 = df_clean_7.copy()

        df_clean_8["leistung"] = df_clean_8.apply(lambda x: self.amend_leistung_col_maserati_mc20(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        df_clean_8["ausstattung"] = None

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        return df_clean_8

    ### Ferrari
    ## SF90
    def clean_ferrari_sf90(self, df_specific_brand):
        """
        A function to clean the data of Ferrari SF90
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        # Add fahrzeugbeschreibung_mod column to replace None values in that column with an empty string
        df_clean_1["fahrzeugbeschreibung_mod"] = df_clean_1["fahrzeugbeschreibung"].apply(lambda x: "" if x is None else x)

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Spotwagen/Coupe, oder Sporwagen/Coupe, Tageszulassung, oder Sporwagen/Coupe, Jaheswagen, oder Sporwagen/Coupe, Neufahrzeug, oder Sporwagen/Coupe, Vorführfahrzeug, dann ändere auf "Coupé".
        # Spalte E = form = Wenn Cabrio/Roadster, oder Cabrio/Roadster, Tageszulassung, oder Cabrio/Roadster, Jaheswagen, oder Cabrio/Roadster, Neufahrzeug, oder Cabrio/Roadster, Vorführfahrzeug, dann ändere auf "Spider".

        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_ferrari_sf90)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Wenn (Leere), oder Automatik,  oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "8-Gang-Doppelkupplungs-Getriebe"

        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_ferrari_sf90)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A = marke = Wenn Spalte D Titel Novitec enthält, dann ändere auf "Ferrari | Novitec"
        # Spalte A = marke = Wenn Spalte D Titel Mansory enthält, dann ändere auf "Ferrari | Mansory"
        # Spalte A = marke = Wenn Spalte D Titel Keyvany enthält, dann ändere auf "Ferrari | Keyvany"
        df_clean_6 = df_clean_5.copy()

        ferrari_sf90_marke_dict = {
            "novitec": "Novitec",
            "mansory": "Mansory",
            "keyvany": "Keyvany"
        }

        for key, value in ferrari_sf90_marke_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "Ferrari"), axis=1)
        
        ###------------------------------###------------------------------###

        ## Amend the `variante` and `leistung` columns
        # Spalte C = Variante = Wenn Marke Ferrari, und modell SF90, und im titel XX steht, und form Coupe, dann ändere auf "SF90 XX Stradale".
        # Spalte C = Variante = Wenn Marke Ferrari, und modell SF90, und im titel XX steht, und form Spider, dann ändere auf "SF90 XX Spider".
        # Spalte G = Leistung = Wenn variante SF90 XX Stradale oder SF90 XX Spider ist, dann ändere Leistung auf "1030".
                
        # Spalte C = Variante = Wenn Marke Ferrari, und modell SF90, und variante (Leere), und im titel Spider oder Spyder steht, dann ändere auf "SF90 Spider".
        # Spalte C = Variante = Wenn Marke Ferrari, und modell SF90, und variante (Leere), und im titel Stradale, dann ändere auf "SF90 Stradale".
        # Spalte C = Variante = Wenn variante (Leere), und Marke Ferrari, und modell SF90, und form Coupe , dann ändere auf "SF90 Stradale".
        # Spalte C = Variante = Wenn variante (Leere), und Marke Ferrari, und modell SF90, und form Spider , dann ändere auf "SF90 Spider".
        # Spalte G = Leistung = Wenn variante SF90 Stradale oder SF90 Spider ist, dann ändere Leistung auf "1000".
        df_clean_7 = df_clean_6.copy()

        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_sf90_stg_1(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_ferrari_sf90_stg_1(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_sf90_stg_2(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_sf90_stg_3(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_ferrari_sf90_stg_2(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D	= Ausstattung = Wenn Marke Ferrari, und modell SF90, und entweder im titel tailor oder in Fahrzeugbeschreibung tailor steht, dann ändere auf "Tailor Made".
        # Neue Spalte D	= Ausstattung = Wenn Marke Ferrari, und modell SF90, und entweder im titel atelier oder in Fahrzeugbeschreibung atelier steht, dann ändere auf "Atelier Car".
        # Neue Spalte D	= Ausstattung = Wenn Marke Ferrari, und modell SF90, und entweder im titel Assetto oder in Fahrzeugbeschreibung Assetto steht, dann ändere auf "Assetto Fiorano".
        df_clean_8 = df_clean_7.copy()

        df_clean_8["ausstattung"] = df_clean_8.apply(lambda x: self.add_ausstattung_col_ferrari_sf90(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        # Drop the fahrzeugbeschreibung_mod column
        df_clean_8 = df_clean_8.drop("fahrzeugbeschreibung_mod", axis=1)

        return df_clean_8

    ### Ferrari
    ## 812
    def clean_ferrari_812(self, df_specific_brand):
        """
        A function to clean the data of Ferrari 812
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        # Add fahrzeugbeschreibung_mod column to replace None values in that column with an empty string
        df_clean_1["fahrzeugbeschreibung_mod"] = df_clean_1["fahrzeugbeschreibung"].apply(lambda x: "" if x is None else x)

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Spotwagen/Coupe, oder Sporwagen/Coupe, Tageszulassung, oder Sporwagen/Coupe, Jaheswagen, oder Sporwagen/Coupe, Neufahrzeug, oder Sporwagen/Coupe, Vorführfahrzeug, dann ändere auf "Coupé".
        # Spalte E = form = Wenn Cabrio/Roadster, oder Cabrio/Roadster, Tageszulassung, oder Cabrio/Roadster, Jaheswagen, oder Cabrio/Roadster, Neufahrzeug, oder Cabrio/Roadster, Vorführfahrzeug, dann ändere auf "Cabrio".

        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_ferrari_812_stg_1)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Wenn (Leere), oder Automatik,  oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "7-Gang-Doppelkupplungs-Getriebe"

        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_ferrari_812)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A = marke = Wenn Spalte D Titel Novitec enthält, dann ändere auf "Ferrari | Novitec"
        # Spalte A = marke = Wenn Spalte D Titel Mansory enthält, dann ändere auf "Ferrari | Mansory"
        df_clean_6 = df_clean_5.copy()

        ferrari_812_marke_dict = {
            "novitec": "Novitec",
            "mansory": "Mansory",
        }

        for key, value in ferrari_812_marke_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "Ferrari"), axis=1)
        
        ###------------------------------###------------------------------###

        ## Amend the `variante` and `leistung` columns
        # Spalte C = Variante = Wenn Marke Ferrari, und modell 812, und entweder im titel  Aperta oder Competizione A oder fahrzeugbeschreibung  Aperta, dann ändere auf "812 Competizione A".
        # Spalte C = Variante = Wenn Marke Ferrari, und modell 812, und entweder im titel  Competi oder fahrzeugbeschreibung Competi, dann ändere auf "812 Competizione".
        # Spate E & G = Form | Leistung = Wenn variante 812 Competizione A, dann änndere Spalte E form auf "Cabrio" und Spalte G leistung auf "830".
        # Spate E & G = Form | Leistung = Wenn variante 812 Competizione, dann änndere Spalte E form auf "Coupé" und Spalte G leistung auf "830".
                
        # Spalte C = Variante = Wenn Marke Ferrari, und modell 812, und im titel  GTS, dann ändere auf "812 GTS".
        # Spalte C = Variante = Wenn Marke Ferrari, und modell 812, und im titel  Superfast, dann ändere auf "812 Superfast".
        # Spalte C = Variante = Wenn variante (Leere), und Marke Ferrari, und modell 812, und Leistung zwischen 780 und 820, und form Coupe , dann ändere auf "812 Superfast".
        # Spalte C = Variante = Wenn variante (Leere), und Marke Ferrari, und modell 812, und Leistung zwischen 780 und 820, und form Cabrio, dann ändere auf "812 GTS".
        # Spate E & G = Form | Leistung = Wenn variante 812 GTS, dann änndere Spalte E form auf "Cabrio" und Spalte G leistung auf "800".
        # Spate E & G = Form | Leistung = Wenn variante 812 Superfast, dann änndere Spalte E form auf "Coupé" und Spalte G leistung auf "800".
        df_clean_7 = df_clean_6.copy()

        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_812_stg_1(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_812_stg_2(x), axis=1)
        df_clean_7["form"] = df_clean_7.apply(lambda x: self.amend_form_col_ferrari_812_stg_2(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_ferrari_812(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_812_stg_3(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_812_stg_4(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_812_stg_5(x), axis=1)
        df_clean_7["form"] = df_clean_7.apply(lambda x: self.amend_form_col_ferrari_812_stg_3(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_ferrari_812(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D	= Ausstattung = Wenn Marke Ferrari, und modell 812, und entweder im titel tailor oder in Fahrzeugbeschreibung tailor steht, dann ändere auf "Tailor Made".
        # Neue Spalte D	= Ausstattung = Wenn Marke Ferrari, und modell 812, und entweder im titel atelier oder in Fahrzeugbeschreibung atelier steht, dann ändere auf "Atelier Car".
        df_clean_8 = df_clean_7.copy()

        df_clean_8["ausstattung"] = df_clean_8.apply(lambda x: self.add_ausstattung_col_ferrari_812(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        # Drop the fahrzeugbeschreibung_mod column
        df_clean_8 = df_clean_8.drop("fahrzeugbeschreibung_mod", axis=1)

        return df_clean_8
    
    ### Ferrari
    ## F12
    def clean_ferrari_F12(self, df_specific_brand):
        """
        A function to clean the data of Ferrari 812
        """
        # Make a copy of df_specific_brand
        df_clean_1 = pd.DataFrame(df_specific_brand.copy())

        # Add fahrzeugbeschreibung_mod column to replace None values in that column with an empty string
        df_clean_1["fahrzeugbeschreibung_mod"] = df_clean_1["fahrzeugbeschreibung"].apply(lambda x: "" if x is None else x)

        ###------------------------------###------------------------------###

        ## Amend the `form` column
        # Spalte E = form = Wenn Spotwagen/Coupe, oder Sporwagen/Coupe, Tageszulassung, oder Sporwagen/Coupe, Jaheswagen, oder Sporwagen/Coupe, Neufahrzeug, oder Sporwagen/Coupe, Vorführfahrzeug, dann ändere auf "Coupé".

        df_clean_2 = df_clean_1.copy()

        df_clean_2["form"] = df_clean_2["form"].apply(self.amend_form_col_ferrari_f12_stg_1)

        ###------------------------------###------------------------------###

        ## Amend the `fahrzeugzustand` column
        # Spalte F = fahrzeugzustand = Wenn (Leere), oder unfallfrei, nicht fahrtauglich, dann ändere auf "Unfallfrei"
        df_clean_3 = df_clean_2.copy()

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col)
        
        ###------------------------------###------------------------------###
        
        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere), dann ändere auf "1"
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col)

        ###------------------------------###------------------------------###

        ## Amend the `getriebe` column
        # Wenn (Leere), oder Automatik,  oder Schaltgetriebe, oder Halbautomatik, dann ändere auf "7-Gang-Doppelkupplungs-Getriebe"

        df_clean_5 = df_clean_4.copy()

        df_clean_5["getriebe"] = df_clean_5["getriebe"].apply(self.amend_getriebe_col_ferrari_f12)

        ###------------------------------###------------------------------###

        ## Amend the `Marke` col based on the `titel` col
        # Spalte A = marke = Wenn Spalte D Titel Novitec enthält, dann ändere auf "Ferrari | Novitec"
        # Spalte A = marke = Wenn Spalte D Titel Mansory enthält, dann ändere auf "Ferrari | Mansory"
        df_clean_6 = df_clean_5.copy()

        ferrari_f12_marke_dict = {
            "novitec": "Novitec",
            "mansory": "Mansory",
        }

        for key, value in ferrari_f12_marke_dict.items():
            df_clean_6["marke"] = df_clean_6.apply(lambda x: self.amend_marke_col_various_brands(x, key, value, "Ferrari"), axis=1)
        
        ###------------------------------###------------------------------###

        ## Amend the `variante` and `leistung` columns
        # Spalte C = Variante = Wenn Marke Ferrari, und modell F12, und im titel tdf steht, und leistung zwischen 770 und 800 ist, dann ändere auf "F12 TDF".
        # Spate E & G = Form | Leistung = Wenn variante F12 TDF, dann ändere Spalte E form auf "Coupé" und Spalte G leistung auf "780".
                
        # Spalte C = Variante = Wenn Marke Ferrari, und modell F12, und leistung ist zwischen 730 und 750, dann ändere auf "F12 Berlinetta".
        # Spate E & G = Form | Leistung = Wenn variante F12 Berlinetta, dann änndere Spalte E form auf "Coupe" und Spalte G leistung auf "740".
        df_clean_7 = df_clean_6.copy()

        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_f12_stg_1(x), axis=1)
        df_clean_7["form"] = df_clean_7.apply(lambda x: self.amend_form_col_ferrari_f12_stg_2(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_ferrari_f12(x), axis=1)
        df_clean_7["variante"] = df_clean_7.apply(lambda x: self.amend_variante_col_ferrari_f12_stg_2(x), axis=1)
        df_clean_7["form"] = df_clean_7.apply(lambda x: self.amend_form_col_ferrari_f12_stg_2(x), axis=1)
        df_clean_7["leistung"] = df_clean_7.apply(lambda x: self.amend_leistung_col_ferrari_f12(x), axis=1)

        ###------------------------------###------------------------------###

        ## Create a new column `Ausstattung`
        # Neue Spalte D	= Ausstattung = Wenn Marke Ferrari, und modell F12, und entweder im titel tailor oder in Fahrzeugbeschreibung tailor steht, dann ändere auf "Tailor Made".
        # Neue Spalte D	= Ausstattung = Wenn Marke Ferrari, und modell F12, und entweder im titel atelier oder in Fahrzeugbeschreibung atelier steht, dann ändere auf "Atelier Car".
        df_clean_8 = df_clean_7.copy()

        df_clean_8["ausstattung"] = df_clean_8.apply(lambda x: self.add_ausstattung_col_ferrari_f12(x), axis=1)

        # Move the Austattung column to be between "variante" and "titel"
        austattung_col = df_clean_8.pop("ausstattung")
        df_clean_8.insert(3, "ausstattung", austattung_col)

        # Drop the fahrzeugbeschreibung_mod column
        df_clean_8 = df_clean_8.drop("fahrzeugbeschreibung_mod", axis=1)

        return df_clean_8
        

def execute_cleaning():
    # Instantiate the classes
    hf = HelperFunctions()
    cf = CleaningFunctions()

    # Load the BQ credentials
    bq_client, bqstorage_client = hf.set_bigquery_credentials()

    # Extract the raw data from BigQuery
    query = """
        SELECT *
        FROM `web-scraping-371310.crawled_datasets.lukas_mobile_de`
        WHERE crawled_timestamp = (SELECT MAX(crawled_timestamp) FROM `web-scraping-371310.crawled_datasets.lukas_mobile_de`)
    """
    df = pd.DataFrame(bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client))

    # Clean the data for specified models
    df_combined = []
    for mod in [
        "Porsche_992", "Lamborghini_Urus", "Aston Martin_DBX",
        "Mercedes-Benz_G 63 AMG", "Mercedes-Benz_SLS AMG", "McLaren_765LT",
        "McLaren_720S", "Masarati_MC20", "Ferrari_SF90", "Ferrari_812", "Ferrari_F12"
    ]:
        marke_to_clean = mod.split("_")[0]
        modell_to_clean = mod.split("_")[1]

        # Filter the data to a specific brand
        df_specific_brand = hf.filter_for_a_specific_brand(df=df, marke=marke_to_clean, modell=modell_to_clean)

        # Clean the data of each brand
        if marke_to_clean == "Porsche" and modell_to_clean == "992":
            logging.info("Cleaning Porsche 992 GT3...")
            df_cleaned = cf.clean_porsche_992_gt3(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Lamborghini" and modell_to_clean == "Urus":
            logging.info("Cleaning Lamborghini Urus...")
            df_cleaned = cf.clean_lamborghini_urus(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Aston Martin" and modell_to_clean == "DBX":
            logging.info("Cleaning Aston Martin DBX...")
            df_cleaned = cf.clean_aston_martin_dbx(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Mercedes-Benz" and modell_to_clean == "G 63 AMG":
            logging.info("Cleaning Mercedes-Benz G 63 AMG...")
            df_cleaned = cf.clean_mercedes_benz_g_63_amg(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Mercedes-Benz" and modell_to_clean == "SLS AMG":
            logging.info("Cleaning Mercedes-Benz SLS AMG...")
            df_cleaned = cf.clean_mercedes_benz_sls_amg(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "McLaren" and modell_to_clean == "765LT":
            logging.info("Cleaning McLaren 765LT...")
            df_cleaned = cf.clean_mclaren_765lt(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "McLaren" and modell_to_clean == "720S":
            logging.info("Cleaning McLaren 720S...")
            df_cleaned = cf.clean_mclaren_720S(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Maserati" and modell_to_clean == "MC20":
            logging.info("Cleaning Maserati MC20...")
            df_cleaned = cf.clean_maserati_mc20(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Ferrari" and modell_to_clean == "SF90":
            logging.info("Cleaning Ferrari SF90...")
            df_cleaned = cf.clean_ferrari_sf90(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Ferrari" and modell_to_clean == "812":
            logging.info("Cleaning Ferrari 812...")
            df_cleaned = cf.clean_ferrari_812(df_specific_brand=df_specific_brand)
        elif marke_to_clean == "Ferrari" and modell_to_clean == "F12":
            logging.info("Cleaning Ferrari F12...")
            df_cleaned = cf.clean_ferrari_F12(df_specific_brand=df_specific_brand)

        # Append the cleaned data to the list
        df_combined.append(df_cleaned)

    # Concatenate the cleaned data
    df_combined = pd.concat(df_combined)

    # Upload the cleaned data to BigQuery
    logging.info("Uploading the cleaned data to BigQuery...")
    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("marke", "STRING"),
            bigquery.SchemaField("modell", "STRING"),
            bigquery.SchemaField("variante", "STRING"),
            bigquery.SchemaField("ausstattung", "STRING"),
            bigquery.SchemaField("titel", "STRING"),
            bigquery.SchemaField("form", "STRING"),
            bigquery.SchemaField("fahrzeugzustand", "STRING"),
            bigquery.SchemaField("leistung", "FLOAT64"),
            bigquery.SchemaField("getriebe", "STRING"),
            bigquery.SchemaField("farbe", "STRING"),
            bigquery.SchemaField("preis", "INT64"),
            bigquery.SchemaField("kilometer", "FLOAT64"),
            bigquery.SchemaField("erstzulassung", "STRING"),
            bigquery.SchemaField("fahrzeughalter", "FLOAT64"),
            bigquery.SchemaField("standort", "STRING"),
            bigquery.SchemaField("fahrzeugbeschreibung", "STRING"),
            bigquery.SchemaField("url_to_crawl", "STRING"),
            bigquery.SchemaField("page_rank", "INT64"),
            bigquery.SchemaField("total_num_pages", "INT64"),
            bigquery.SchemaField("crawled_timestamp", "TIMESTAMP")
        ]
    )
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # Upload the table
    bq_client.load_table_from_dataframe(
        dataframe=df_combined,
        destination="web-scraping-371310.crawled_datasets.lukas_mobile_de_cleaned",
        job_config=job_config
    ).result()