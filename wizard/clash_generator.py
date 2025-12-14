import os

def generate_clash_config():
    config = """
port: 7890
socks-port: 7891
mixed-port: 7893
allow-lan: false
mode: rule
log-level: info
ipv6: false
external-controller: 127.0.0.1:9090

dns:
  enable: true
  listen: 0.0.0.0:1053
  enhanced-mode: fake-ip
  nameserver:
    - 8.8.8.8
    - 1.1.1.1

tun:
  enable: true
  stack: system
  auto-route: true
  auto-detect-interface: true
  dns-hijack:
    - any:53

proxies:
  - name: "Tuman-VPN"
    type: http # Мы используем HTTP прокси, который уже есть в tumanvpn
    server: 127.0.0.1
    port: 8080

proxy-groups:
  - name: "Proxy"
    type: select
    proxies:
      - "Tuman-VPN"
      - "DIRECT"

rules:
  # Российские сайты - напрямую (чтобы работали быстро и без ВПН)
  - DOMAIN-SUFFIX,ru,DIRECT
  - DOMAIN-SUFFIX,yandex.ru,DIRECT
  - DOMAIN-SUFFIX,vk.com,DIRECT
  - DOMAIN-SUFFIX,dzen.ru,DIRECT
  
  # Сначала Yandex напрямую (ОБЯЗАТЕЛЬНО, инче будет вечный цикл)
  # Это транспорт для VPN, он не может идти через VPN
  - DOMAIN-SUFFIX,yandex.ru,DIRECT
  - DOMAIN-SUFFIX,yandex.net,DIRECT
  - DOMAIN-SUFFIX,storage.yandexcloud.net,DIRECT
  - DOMAIN-SUFFIX,cloud-api.yandex.net,DIRECT

  # Все остальное - через VPN
  - MATCH,Tuman-VPN

"""
    output_path = "data/tuman_clash_v3.yaml"
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    with open(output_path, "w") as f:
        f.write(config.strip())
    
    print(f"[OK] FINAL CONFIG (V3) создан: {os.path.abspath(output_path)}")
    print("")
    print("ВАЖНО: Ваш TumanVPN должен быть ЗАПУЩЕН в терминале.")
    print("")
    print("ИНСТРУКЦИЯ:")
    print("1. В терминале запустите: ./setup.sh -> 2 (Client) -> Запустить")
    print("2. В ClashX.Meta загрузите этот файл: tuman_clash_v3.yaml")
    print("3. ОБЯЗАТЕЛЬНО выберите Mode: RULE (это критически важно для работы)")
    print("   (В этом конфиге Rule настроен перехватывать ВСЁ, как Global, но безопасно)")

if __name__ == "__main__":
    generate_clash_config()
