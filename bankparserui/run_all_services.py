#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для запуска всех сервисов проекта:
- FastAPI API сервер (Swagger на /docs)
- UI для загрузки выписок
- UI для поиска налогоплательщика
"""

import subprocess
import sys
import time
import socket
from pathlib import Path

def run_api_server():
    """Запуск FastAPI сервера с Swagger"""
    print("🚀 Запуск FastAPI сервера (порт 8000)...")
    print("   Swagger UI: http://localhost:8000/docs")
    print("   API: http://localhost:8000")
    
    api_path = Path(__file__).parent / "src" / "api" / "app.py"
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "src.api.app:app",
        "--host", "0.0.0.0",
        "--port", "8000",
        "--reload"
    ]
    return subprocess.Popen(cmd, cwd=Path(__file__).parent)


def run_upload_ui():
    """Запуск UI для загрузки выписок"""
    print("🚀 Запуск UI для загрузки выписок (порт 8502)...")
    
    upload_app_path = Path(__file__).parent / "src" / "ui" / "upload_app.py"
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(upload_app_path),
        "--server.port=8502",
        "--server.address=0.0.0.0",
        "--server.headless=true"
    ]
    return subprocess.Popen(
        cmd,
        cwd=Path(__file__).parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )


def run_taxpayer_search_ui():
    """Запуск UI для поиска налогоплательщика"""
    print("🚀 Запуск UI для поиска налогоплательщика (порт 8503)...")
    print("   URL: http://localhost:8503")
    
    taxpayer_app_path = Path(__file__).parent / "src" / "ui" / "taxpayer_search_app.py"
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(taxpayer_app_path),
        "--server.port=8503",
        "--server.address=0.0.0.0",
        "--server.headless=true"
    ]
    return subprocess.Popen(
        cmd,
        cwd=Path(__file__).parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )


def is_port_in_use(port: int) -> bool:
    """Проверка, занят ли порт"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def kill_process_on_port(port: int) -> bool:
    """Попытка освободить порт"""
    try:
        result = subprocess.run(
            ['lsof', '-ti', f':{port}'],
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                try:
                    subprocess.run(['kill', '-9', pid], check=False)
                    print(f"   ⚠️  Освобожден порт {port} (процесс {pid})")
                except:
                    pass
            return True
    except:
        pass
    return False


def main():
    """Запуск всех сервисов"""
    print("=" * 60)
    print("Запуск всех сервисов проекта")
    print("=" * 60)
    print()
    
    # Проверка и освобождение портов
    ports_to_check = [8000, 8502, 8503]
    for port in ports_to_check:
        if is_port_in_use(port):
            print(f"⚠️  Порт {port} занят, пытаюсь освободить...")
            kill_process_on_port(port)
            time.sleep(1)
    
    processes = []
    
    try:
        # Запуск всех сервисов
        processes.append(run_api_server())
        time.sleep(2)  # Небольшая задержка между запусками
        
        processes.append(run_upload_ui())
        time.sleep(2)
        
        processes.append(run_taxpayer_search_ui())
        time.sleep(2)
        
        # Проверка, что все процессы запустились
        time.sleep(3)
        failed_processes = []
        for i, proc in enumerate(processes):
            if proc.poll() is not None:
                failed_processes.append(i)
                try:
                    stdout, _ = proc.communicate(timeout=1)
                    if stdout:
                        # Security: Don't leak full output (information leak)
                        print(f"\n⚠️  Процесс {i} завершился. Проверьте логи.")
                except:
                    pass
        
        if failed_processes:
            print(f"\n❌ Некоторые сервисы не запустились: {failed_processes}")
            print("Проверьте логи выше для деталей")
        
        print()
        print("=" * 60)
        print("✅ Все сервисы запущены!")
        print("=" * 60)
        print()
        print("Доступные сервисы:")
        print("  📚 Swagger UI:     http://localhost:8000/docs")
        print("  🔌 API:            http://localhost:8000")
        print("  📤 Загрузка:       http://localhost:8502")
        print("  🔍 Поиск ИП/ЮЛ:    http://localhost:8503")
        print()
        print("Нажмите Ctrl+C для остановки всех сервисов")
        print("=" * 60)
        
        # Ожидание завершения с периодической проверкой
        try:
            while True:
                time.sleep(1)
                # Проверяем, что процессы еще работают
                for i, proc in enumerate(processes):
                    if proc.poll() is not None:
                        print(f"\n⚠️  Процесс {i} завершился неожиданно")
                        try:
                            proc.communicate(timeout=1)
                        except:
                            pass
        except KeyboardInterrupt:
            pass
            
    except KeyboardInterrupt:
        print("\n\n🛑 Остановка всех сервисов...")
        for proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                proc.kill()
        print("✅ Все сервисы остановлены")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Ошибка при запуске: {e}")
        print(f"   Тип ошибки: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        
        # Вывести ошибки из процессов Streamlit, если они есть
        for i, proc in enumerate(processes):
            try:
                if proc.poll() is not None:
                    proc.communicate(timeout=1)
            except Exception:
                pass
        
        for proc in processes:
            try:
                proc.terminate()
            except:
                pass
        sys.exit(1)


if __name__ == "__main__":
    main()
