from mobile_de_selenium_code_prod_local_single_func import mobile_de_local_single_func

def main():
    mobile_de_local_single_func(
        category="cat_2",
        car_list=[
            "Audi"
        ],
        modell_list=[
            "quattro",
            "R8",
            "RS2",
            "RS3",
            "RS4",
            "RS5",
            "RS6",
            "RS7",
            "RSQ3",
            "RSQ8",
            "    TT RS",
            "e-tron",
        ]
    )

if __name__ == '__main__':
    main()