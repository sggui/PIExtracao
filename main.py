import requests
import json

def fetch_data(page):
    url = f'https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/ofertas%2Ffeiroes%2Flojaoficialtoyota%2Fcarros-usados%2Festoque%2Ftoyota%26%3Flkid%3D2103%26tipoveiculo%3Dcarros-usados%26marca1%3DTOYOTA%26feirao%3DLoja%2520Oficial%2520Toyota%26estadocidade%3DS%25C3%25A3o%2520Paulo&actualPage={page}&displayPerPage=24&order=1&showMenu=true&showCount=true&showBreadCrumb=true&testAB=false&returnUrl=false&pandora=false'

    # Headers necessários para simular o acesso como navegador
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Cookie': 'WebMotorsLastSearches=...; AMCVS_3ADD33055666F1A47F000101%40AdobeOrg=1; at_check=true; WebMotorsVisitor=1; WebMotorsLocation=...; WMLastFilterSearch=...; WebMotorsSearchDataLayer=...; mbox=PC#...|session#...#...; AMCV_3ADD33055666F1A47F000101%40AdobeOrg=...;vVersion=5.5.0',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'X-Channel-Id': 'webmotors.buyer.desktop.ui',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera GX";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    response = requests.get(url, headers=headers)
    return response

def main():
    page = 1
    all_cars = []

    while True:
        response = fetch_data(page)
        if response.status_code != 200:
            print(f"Erro ao fazer requisição: {response.status_code} - {response.text}")
            break
        
        data = response.json()
        if not data.get('SearchResults'):
            print("Não há mais resultados.")
            break

        for item in data.get('SearchResults', []):
            car_info = {
                "Specification": item['Specification'],
                "Seller": item['Seller'],
                "Prices": item['Prices']
            }
            all_cars.append(car_info)

        print(f"Dados da página {page} coletados com sucesso.")
        page += 1

    # Salva os dados em um arquivo JSON
    with open('cars_data_complete.json', 'w') as f:
        json.dump(all_cars, f, indent=4)

    print('Todos os dados foram coletados e salvos com sucesso!')

if __name__ == "__main__":
    main()
