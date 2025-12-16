#!/bin/bash
set -e
cd "$(dirname "$0")"

# Определяем Python
if command -v python3 &> /dev/null; then
    PY=python3
elif command -v python &> /dev/null; then
    PY=python
else
    PY=""
fi

echo ""
echo "+------------------------------------------+"
echo "|          TumanVPN Setup                  |"
echo "+------------------------------------------+"
echo ""

# === ВЫБОР РОЛИ ===
echo "Что настраиваем?"
echo "  [1] Сервер (первая настройка, ввод cookies)"
echo "  [2] Клиент (есть строка подключения)"
echo ""
read -p "Выбор [1/2]: " role_choice

case $role_choice in
    1)
        # ========== СЕРВЕР ==========
        echo ""
        echo "Вставьте cookies из disk.yandex.ru: "
        echo "1. Используйте расширение Cookie-Editor https://chromewebstore.google.com/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm"
        echo "2. Перейдите на https://disk.yandex.ru/notes/"
        echo "3. Откройте Cookie-Editor и выберите Export As: Netscape"
        echo "4. Вставьте сюда экспортированные cookies"
        echo "(потом два раза нажмите Enter):"
        echo ""
        
        COOKIES=$(sed '/^$/q')
        
        COOKIES=$(echo "$COOKIES" | sed 's/[[:space:]]*$//')
        
        if [[ -z "$COOKIES" ]]; then
            echo "[!] Cookies не введены"
            exit 1
        fi
        
        # Сохраняем cookies
        echo "$COOKIES" > data/diskcookies.txt
        echo "[OK] Cookies сохранены"
        
        # Генерируем encryption key
        ENCRYPTION_KEY=$(LC_ALL=C tr -dc 'A-Za-z0-9_-' < /dev/urandom | head -c 32)
        
        # Создаём конфиг
        cat > data/config.json << EOF
{
  "mode": "yanotes",
  "storage": {
    "cookies_path": "data/diskcookies.txt",
    "encryption_key": "$ENCRYPTION_KEY"
  },
  "settings": {
    "proxy_port": 8080,
    "timeout": 120,
    "chunk_size": 500000,
    "chunk_idle_timeout": 0.1,
    "poll_interval": 0.1,
    "cleanup_chunks": true,
    "tunnel_idle_timeout": 120
  }
}
EOF
        echo "[OK] Конфиг создан"
        
        # === ПРОВЕРКА ЗАМЕТОК ===
        echo ""
        echo "[*] Проверка заметок Yandex Notes..."
        
        # Проверяем наличие Python
        if [[ -z "$PY" ]]; then
            echo "[!] Python не найден, пропускаем проверку заметок"
        else
            # Создаём venv если нет
            if [[ ! -d ".venv" ]]; then
                echo "[*] Создаём виртуальное окружение..."
                $PY -m venv .venv
            fi
            source .venv/bin/activate
            
            # Проверяем httpx
            if ! python -c "import httpx" 2>/dev/null; then
                echo "[*] Устанавливаем зависимости..."
                pip install httpx -q
            fi
            
            # Запускаем проверку
            POOL_CHECK=$(python wizard/yanotes_setup.py check 2>/dev/null || echo "ERROR")
            
            if [[ "$POOL_CHECK" == "NO_POOL" ]]; then
                echo "[!] Файл пулов заметок не найден"
                echo ""
                read -p "Создать заметки? [Y/n]: " create_choice
                if [[ ! "$create_choice" =~ ^[Nn] ]]; then
                    echo "[*] Создание заметок (это займёт ~30 сек)..."
                    python wizard/yanotes_setup.py create
                    if [[ $? -eq 0 ]]; then
                        echo "[OK] Заметки созданы!"
                    else
                        echo "[!] Ошибка создания заметок"
                    fi
                fi
            elif [[ "$POOL_CHECK" == ERROR* ]]; then
                echo "[!] Ошибка проверки: $POOL_CHECK"
            else
                # Парсим результат
                POOL_INFO=$(echo "$POOL_CHECK" | grep "^POOL_FOUND" | cut -d: -f2,3)
                CLIENT_COUNT=$(echo "$POOL_INFO" | cut -d: -f1)
                SERVER_COUNT=$(echo "$POOL_INFO" | cut -d: -f2)
                ACCESS_STATUS=$(echo "$POOL_CHECK" | grep "^ACCESS")
                
                echo "[OK] Найден пул заметок: клиент=$CLIENT_COUNT, сервер=$SERVER_COUNT"
                
                if [[ "$ACCESS_STATUS" == "ACCESS_OK" ]]; then
                    echo "[OK] Доступ к заметкам: есть"
                elif [[ "$ACCESS_STATUS" == "ACCESS_NONE" ]]; then
                    echo "[!] Доступ к заметкам: НЕТ (cookies устарели?)"
                else
                    echo "[!] Доступ к заметкам: частичный ($ACCESS_STATUS)"
                fi
                
                echo ""
                read -p "Пересоздать заметки? [y/N]: " recreate_choice
                if [[ "$recreate_choice" =~ ^[Yy] ]]; then
                    echo "[*] Создание заметок (это займёт ~30 сек)..."
                    python wizard/yanotes_setup.py create
                    if [[ $? -eq 0 ]]; then
                        echo "[OK] Заметки созданы!"
                    else
                        echo "[!] Ошибка создания заметок"
                    fi
                fi
            fi
        fi
        
        # Генерируем строку подключения (cookies + config + pool)
        CONN_DATA=$(cat data/diskcookies.txt; echo "---CONFIG---"; cat data/config.json; echo "---POOL---"; cat data/yanotes_pool.json)
        CONN_STRING="tuman://$(echo "$CONN_DATA" | base64 | tr -d '\n')"
        
        echo ""
        echo "=========================================="
        echo "СТРОКА ПОДКЛЮЧЕНИЯ ДЛЯ КЛИЕНТА:"
        echo "=========================================="
        echo ""
        echo "$CONN_STRING"
        echo ""
        echo "=========================================="
        echo ""
        
        # Запуск сервера
        echo "Запустить сервер?"
        echo "  [1] Да, в фоне (up -d) - рекомендуется"
        echo "  [2] Да, интерактивно (up)"
        echo "  [3] Нет"
        read -p "Выбор [1]: " run_choice
        run_choice="${run_choice:-1}"
        
        if [[ "$run_choice" != "3" ]]; then
            if docker compose version &> /dev/null 2>&1; then
                COMPOSE="docker compose"
            else
                COMPOSE="docker-compose"
            fi
            cd server
            echo "[*] Запуск..."
            
            if [[ "$run_choice" == "2" ]]; then
                $COMPOSE up --build
            else
                $COMPOSE up -d --build
                echo ""
                echo "[OK] Сервер запущен в фоне!"
                echo "    Логи: cd server && docker compose logs -f"
                echo "    Стоп: cd server && docker compose down"
            fi
        fi
        ;;
        
    2)
        # ========== КЛИЕНТ ==========
        echo ""
        echo "Убедитесь, что строка подключения (tuman://...) скопирована в буфер обмена"
        read -p "Использовать строку из буфера? Нажмите [Enter]:"
        
        # Получаем из буфера обмена (macOS: pbpaste, Linux: xclip)
        if command -v pbpaste &> /dev/null; then
            CONN_STRING=$(pbpaste)
        elif command -v xclip &> /dev/null; then
            CONN_STRING=$(xclip -selection clipboard -o)
        elif command -v xsel &> /dev/null; then
            CONN_STRING=$(xsel --clipboard --output)
        else
            echo "[!] Не найден pbpaste/xclip/xsel"
            echo "    Сохраните строку в файл и запустите:"
            echo "    cat connection.txt | ./setup.sh --import"
            exit 1
        fi
        
        if [[ -z "$CONN_STRING" ]]; then
            echo "[!] Буфер обмена пуст"
            exit 1
        fi
        
        if [[ "$CONN_STRING" != tuman://* ]]; then
            echo "[!] Неверный формат. Строка должна начинаться с tuman://"
            exit 1
        fi
        
        echo "[OK] Строка получена из буфера обмена"
        
        # Убираем префикс tuman://
        ENCODED="${CONN_STRING#tuman://}"
        
        # Декодируем base64
        DECODED=$(echo "$ENCODED" | base64 -d 2>/dev/null) || {
            echo "[!] Ошибка декодирования строки подключения"
            exit 1
        }
        
        # Разделяем cookies, config и pool
        echo "$DECODED" | awk '/^---CONFIG---$/{exit} {print}' > data/diskcookies.txt
        echo "$DECODED" | awk '/^---CONFIG---$/{found=1; next} /^---POOL---$/{exit} found' > data/config.json
        echo "$DECODED" | awk '/^---POOL---$/{found=1; next} found' > data/yanotes_pool.json
        
        echo "[OK] Конфигурация получена!"
        echo "    - cookies: $(wc -l < data/diskcookies.txt | tr -d ' ') строк"
        echo "    - config: data/config.json"
        echo "    - pool: $(cat data/yanotes_pool.json | $PY -c 'import sys,json; d=json.load(sys.stdin); print(f"{len(d.get(\"client_pool\",[]))} клиент, {len(d.get(\"server_pool\",[]))} сервер")' 2>/dev/null || echo 'загружен')"
        
        # Запуск клиента
        echo ""
        read -p "Запустить? [Y/n]: " run_choice
        if [[ ! "$run_choice" =~ ^[Nn] ]]; then
            if docker compose version &> /dev/null 2>&1; then
                COMPOSE="docker compose"
            else
                COMPOSE="docker-compose"
            fi
            cd client
            echo "[*] Запуск..."
            $COMPOSE up --build
        fi
        ;;
        
    *)
        echo "[!] Неверный выбор"
        exit 1
        ;;
esac
