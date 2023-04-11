from mobile_de_selenium_code_prod_local_single_func import mobile_de_local_single_func

def main():
    mobile_de_local_single_func(
        category="cat_5",
        car_list=[
            "Corvette",
            "Dodge",
            "Nissan",
            "Ford",
            "Alfa Romeo",
            "Jaguar",
            "Lexus",
            "Lotus",
            "Maserati",
            "Honda"
        ],
        modell_list=[
            # Corvette
            "C8",
            
            # Dodge
            "Viper",

            # Nissan
            "GT-R",
            
            # Ford
            "GT",

            # Alfa Romeo
            "4C",
            "8C",
            "Giulia",

            # Jaguar
            "F-Type",
            "Andere",

            # Lexus
            "LFA",

            # Lotus
            "Exige",
            "Emira",
            "Andere",

            # Maserati
            "MC20",

            # Honda
            "NSX"
        ]
    )

if __name__ == '__main__':
    main()