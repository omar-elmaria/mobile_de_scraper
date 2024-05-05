# Import packages
import pandas as pd

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
        if x.lower().find("suv") != -1 or x.lower().find("geländewagen") != -1\
        or x.lower().find("pickup") != -1 or x.lower().find("andere") != -1\
        or x.lower().find("sportwagen") != -1 or x.lower().find("coup") != -1:
            return "Super Sport Utility Vehicle"
        else:
            return x
    
    def amend_form_col_aston_martin_dbx(self, x):
        if x.lower().find("suv") != -1 or x.lower().find("geländewagen") != -1\
        or x.lower().find("pickup") != -1 or x.lower().find("andere") != -1\
        or x.lower().find("sportwagen") != -1 or x.lower().find("coup") != -1:
            return "SUV"
        else:
            return x
        
    ###------------------------------###------------------------------###
    
    ## Fahrzeugzustand column helper functions 
    def amend_fahrzeugzustand_col_porsche_992_gt3(self, x):
        """
        A function to amend the `fahrzeugzustand` column for Porsche 992 GT3
        """
        if x == "" or x is None or pd.isnull(x):
            return "Unfallfrei"
        elif x.lower().find("unfallfrei") != -1:
            return "Unfallfrei"
        else:
            return x
        
    def amend_fahrzeugzustand_col_lamborghini_urus(self, x):
        """
        A function to amend the `fahrzeugzustand` column for Lamborghini Urus
        """
        if x == "" or x is None or pd.isnull(x):
            return "Unfallfrei"
        elif x.lower().find("unfallfrei") != -1:
            return "Unfallfrei"
        else:
            return x
    
    def amend_fahrzeugzustand_col_aston_martin_dbx(self, x):
        """
        A function to amend the `fahrzeugzustand` column for Aston Martin DBX
        """
        if x == "" or x is None or pd.isnull(x):
            return "Unfallfrei"
        elif x.lower().find("unfallfrei") != -1:
            return "Unfallfrei"
        else:
            return x
        
    ###------------------------------###------------------------------###

    ## Kilometer column helper functions
    def amend_kilometer_col_porsche_992_gt3(self, x):
        """
        A function to amend the `kilometer` column for Porsche 992 GT3
        """
        if x == "" or x is None or pd.isnull(x):
            return 1
        else:
            return x
        
    def amend_kilometer_col_lamborghini_urus(self, x):
        """
        A function to amend the `kilometer` column for Lamborghini Urus
        """
        if x == "" or x is None or pd.isnull(x):
            return 1
        else:
            return x
    
    def amend_kilometer_col_aston_martin_dbx(self, x):
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
        if x["marke"] == "Aston Martin" and x["modell"] == "DBX"\
        and x["variante"] == "DBX V8" and x["form"] == "SUV" and x["fahrzeugzustand"].lower() == "unfallfrei"\
        and x["leistung"] == 550 and (x["titel"].lower().find("1913") != -1 or x["fahrzeugbeschreibung_mod"].lower().find("1913") != -1):
            return "1913 Edition"
        else:
            return None

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

        # Apply the function amend_fahrzeugzustand_col_porsche_992_gt3 on fahrzeugzustand using the apply method
        df_clean_4["fahrzeugzustand"] = df_clean_4["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col_porsche_992_gt3)

        ###------------------------------###------------------------------###

        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_5 = df_clean_4.copy()
        df_clean_5["kilometer"] = df_clean_5["kilometer"].apply(self.amend_kilometer_col_porsche_992_gt3)
        
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

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col_lamborghini_urus)

        ###------------------------------###------------------------------###

        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col_lamborghini_urus)

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

        df_clean_3["fahrzeugzustand"] = df_clean_3["fahrzeugzustand"].apply(self.amend_fahrzeugzustand_col_aston_martin_dbx)

        ###------------------------------###------------------------------###

        ## Amend the `kilometer` column
        # Spalte K = kilometer = Wenn (Leere) abgebildet wird, dann auf „1“ ändern. (Info: Sind oftmals Neuwagen und haben Werkskilometer)
        df_clean_4 = df_clean_3.copy()

        df_clean_4["kilometer"] = df_clean_4["kilometer"].apply(self.amend_kilometer_col_aston_martin_dbx)

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

        return df_clean_8
