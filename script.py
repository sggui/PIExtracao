import os
import requests
import json
import time
import random
import logging

# Configura o logger
logging.basicConfig(
    filename='log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Caminho do arquivo de checkpoint
checkpoint_file = 'checkpoint.json'

def save_checkpoint(page, car_index):
    checkpoint_data = {'page': page, 'car_index': car_index}
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)
    logging.info(f"Checkpoint salvo: página {page}, carro {car_index}.")

def load_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    return None

def fetch_data(page):
    url = f'https://www.webmotors.com.br/api/search/car?url=https://www.webmotors.com.br/carros-usados%2Festoque%3Flkid%3D1000%26estadocidade%3DS%25C3%25A3o%2520Paulo&actualPage={page}&displayPerPage=24&order=1&showMenu=true&showCount=true&showBreadCrumb=true&testAB=false&returnUrl=false&pandora=false'
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

def fetch_car_details(unique_id):
    url_id = f'https://www.webmotors.com.br/api/detail/averageprice/car/{unique_id}?pandora=false'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0'
    }

    response = requests.get(url_id, headers=headers)
    return response

def get_last_page():
    if not os.path.exists('storage'):
        os.makedirs('storage')
        return 1

    json_files = [f for f in os.listdir('storage') if f.endswith('.json')]
    if not json_files:
        return 1
    
    last_file = max(json_files, key=lambda x: int(x.replace('.json', '')))
    last_page = int(last_file.replace('.json', ''))
    
    return last_page + 1

def main():
    checkpoint = load_checkpoint()
    if checkpoint:
        page = checkpoint['page']
        car_start_index = checkpoint['car_index']
        logging.info(f"Retomando do ponto de falha na página {page}, carro {car_start_index}.")
    else:
        page = get_last_page()
        car_start_index = 1

    while True:
        logging.info(f"Fetching data for page {page}...")
        response = fetch_data(page)
        if response.status_code != 200:
            logging.error(f"Erro ao fazer requisição: {response.status_code} - {response.text}")
            break
        
        data = response.json()
        if not data.get('SearchResults'):
            logging.info("Não há mais resultados.")
            break

        # Cria a pasta para a página
        page_folder = os.path.join('storage', f'{page}')
        if not os.path.exists(page_folder):
            os.makedirs(page_folder)

        # Salva o arquivo JSON da página inteira
        page_json_path = os.path.join('storage', f'{page}.json')
        with open(page_json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f'Dados da página {page} salvos com sucesso.')

        total_cars = len(data['SearchResults'])

        # Para cada carro, busque os detalhes e salve em um arquivo separado
        for index, car in enumerate(data['SearchResults'][car_start_index-1:], start=car_start_index):
            unique_id = car.get('UniqueId')
            details_response = fetch_car_details(unique_id)
            if details_response.status_code == 200:
                car['Details'] = details_response.json()
            else:
                logging.error(f"Erro ao buscar detalhes do carro com UniqueId {unique_id}: {details_response.status_code}")
                save_checkpoint(page, index)
                return  # Interrompe o script

            # Salva os dados do carro em um arquivo JSON separado dentro da pasta da página
            car_json_path = os.path.join(page_folder, f'{index}.json')
            with open(car_json_path, 'w', encoding='utf-8') as f:
                json.dump(car, f, indent=4, ensure_ascii=False)

            logging.info(f'Detalhes do carro {index} da página {page} salvos com sucesso.')

            # Salva o checkpoint após cada carro processado com sucesso
            save_checkpoint(page, index)

            # Gera um intervalo de espera aleatório entre 5 e 10 segundos entre as requisições de carros
            sleep_time_car = random.randint(5, 10)
            logging.info(f"Aguardando {sleep_time_car} segundos antes de buscar o próximo carro...")
            time.sleep(sleep_time_car)

        # Verifica se todos os carros da página foram processados
        if index == total_cars:
            car_start_index = 1  # Reinicia o índice de carros para a próxima página
            logging.info(f'Dados da página {page} e seus carros coletados e salvos com sucesso.')

            # Gera um intervalo de espera aleatório entre 15 e 20 segundos entre as requisições de páginas
            sleep_time_page = random.randint(15, 20)
            logging.info(f"Aguardando {sleep_time_page} segundos antes de continuar para a próxima página...")
            time.sleep(sleep_time_page)

            page += 1
        else:
            logging.info(f"A página {page} ainda não foi totalmente processada.")
            save_checkpoint(page, car_start_index)

    logging.info('Todos os dados foram coletados e salvos com sucesso!')

if __name__ == "__main__":
    main()
