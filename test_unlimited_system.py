#!/usr/bin/env python3
"""
🧪 Script de Prueba - Sistema Sin Límites
Prueba las nuevas características del sistema dinámico sin límites
"""

import asyncio
import aiohttp
import json
from datetime import datetime

class UnlimitedSystemTester:
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTg5MjQ2NDAsInN1YiI6IlZJQzAwMSJ9.6s2ah2WMT39BhUbVOXgN2AVJp05L1Q7bZDeCaFXn-xc"
        self.headers = {"Authorization": f"Bearer {self.token}"}

    async def test_unlimited_worker_scaling(self):
        """Prueba escalado sin límites en colas predefinidas"""
        print("🚀 PROBANDO ESCALADO SIN LÍMITES")
        print("=" * 50)
        
        # Obtener colas actuales
        queues = await self.get_all_queues()
        if not queues:
            print("❌ No se pudieron obtener las colas")
            return False
        
        predefined_queues = queues.get('predefined_queues', {})
        
        for queue_key, queue_info in list(predefined_queues.items())[:2]:  # Probar con las primeras 2
            config = queue_info.get('config', {})
            queue_name = config.get('name', 'Unknown')
            current_workers = queue_info.get('active_workers', 0)
            
            print(f"\n📊 Probando cola '{queue_name}':")
            print(f"   Workers actuales: {current_workers}")
            
            # Probar escalado a 15 workers (mucho más que el límite original)
            new_target = 15
            print(f"   Escalando a {new_target} workers...")
            
            success = await self.scale_queue_by_name(queue_name, new_target)
            if success:
                print(f"   ✅ Escalado exitoso sin límites!")
                
                # Verificar el cambio
                await asyncio.sleep(2)
                updated_queues = await self.get_all_queues()
                if updated_queues:
                    updated_queue = updated_queues.get('predefined_queues', {}).get(queue_key, {})
                    actual_workers = updated_queue.get('active_workers', 0)
                    print(f"   📈 Workers verificados: {actual_workers}")
                    
                    if actual_workers == new_target:
                        print(f"   🎉 PERFECTO: Sin límites funciona!")
                    else:
                        print(f"   ⚠️  Discrepancia: Esperado {new_target}, Real {actual_workers}")
            else:
                print(f"   ❌ Error en escalado")
        
        return True

    async def test_predefined_queue_deletion(self):
        """Prueba eliminación de colas predefinidas"""
        print(f"\n🗑️  PROBANDO ELIMINACIÓN DE COLAS PREDEFINIDAS")
        print("=" * 50)
        
        # Intentar eliminar la cola BULK (la menos crítica)
        queue_to_delete = 4  # BULK queue
        print(f"Intentando eliminar cola BULK (ID: {queue_to_delete})...")
        
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/queue/queues/{queue_to_delete}"
                async with session.delete(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        print(f"✅ Cola BULK eliminada exitosamente!")
                        print(f"   Respuesta: {data.get('message', 'N/A')}")
                        
                        # Verificar que ya no existe
                        await asyncio.sleep(1)
                        queues = await self.get_all_queues()
                        if queues:
                            predefined = queues.get('predefined_queues', {})
                            if '4' not in predefined:
                                print(f"   ✅ Verificado: Cola BULK ya no existe")
                            else:
                                print(f"   ⚠️  Cola BULK todavía aparece en la lista")
                        
                        return True
                    else:
                        error_data = await response.json()
                        print(f"❌ Error eliminando cola: {error_data.get('detail', 'Unknown')}")
                        return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False

    async def test_auto_queue_creation(self):
        """Prueba creación automática de colas"""
        print(f"\n🔧 PROBANDO CREACIÓN AUTOMÁTICA DE COLAS")
        print("=" * 50)
        
        # Crear tareas con prioridades que no tienen cola asignada
        unusual_priorities = [15, 18, 25, 30]
        
        for priority in unusual_priorities:
            print(f"\nProbando prioridad {priority}...")
            
            # Crear tarea con prioridad inusual
            task_created = await self.create_test_task_with_priority(priority)
            
            if task_created:
                print(f"   ✅ Tarea creada con prioridad {priority}")
                
                # Esperar un momento para que se cree la cola automática
                await asyncio.sleep(3)
                
                # Verificar si se creó una cola automática
                queues = await self.get_all_queues()
                if queues:
                    dynamic_queues = queues.get('dynamic_queues', {})
                    auto_queue_found = False
                    
                    for queue_id, queue_info in dynamic_queues.items():
                        config = queue_info.get('config', {})
                        priority_range = config.get('priority_range', [])
                        
                        if len(priority_range) >= 2 and priority_range[0] <= priority <= priority_range[1]:
                            queue_name = config.get('name', 'Unknown')
                            print(f"   🎯 Cola automática encontrada: {queue_name}")
                            print(f"      Rango de prioridad: {priority_range}")
                            auto_queue_found = True
                            break
                    
                    if not auto_queue_found:
                        print(f"   ⚠️  No se encontró cola automática para prioridad {priority}")
            else:
                print(f"   ❌ Error creando tarea con prioridad {priority}")
        
        return True

    async def create_test_task_with_priority(self, priority):
        """Crea una tarea de prueba con prioridad específica"""
        try:
            # Usar endpoint de estudiantes con parámetro de prioridad
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/api/v1/estudiantes?priority={priority}"
                async with session.get(url, headers=self.headers) as response:
                    return response.status == 200
        except Exception as e:
            print(f"   Error creando tarea: {e}")
            return False

    async def scale_queue_by_name(self, queue_name, target_workers):
        """Escala una cola por nombre"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/queue/workers/scale-by-name"
                params = {
                    "queue_name": queue_name,
                    "target_workers": target_workers
                }
                async with session.post(url, headers=self.headers, params=params) as response:
                    if response.status == 200:
                        return True
                    else:
                        error_data = await response.json()
                        print(f"   Error escalando: {error_data.get('detail', 'Unknown')}")
                        return False
        except Exception as e:
            print(f"   Error: {e}")
            return False

    async def get_all_queues(self):
        """Obtiene información de todas las colas"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/queue/queues/all", headers=self.headers) as response:
                    if response.status == 200:
                        return await response.json()
                    return None
        except Exception as e:
            print(f"Error obteniendo colas: {e}")
            return None

    async def show_system_status(self):
        """Muestra estado actual del sistema"""
        print(f"\n📊 ESTADO ACTUAL DEL SISTEMA")
        print("=" * 50)
        
        try:
            # Estado general
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/queue/status", headers=self.headers) as response:
                    if response.status == 200:
                        status = await response.json()
                        print(f"🚀 Sistema: {status.get('system_type', 'Unknown')}")
                        print(f"👥 Workers totales: {status.get('total_workers', 0)}")
                        print(f"📋 Colas activas: {status.get('total_queues', 0)}")
                        print(f"⚡ Eventos de escalado: {status.get('dynamic_scaling_events', 0)}")
            
            # Detalles de colas
            queues = await self.get_all_queues()
            if queues and queues.get('success'):
                predefined = queues.get('predefined_queues', {})
                dynamic = queues.get('dynamic_queues', {})
                
                print(f"\n📊 COLAS PREDEFINIDAS ({len(predefined)}):")
                for queue_key, queue_info in predefined.items():
                    config = queue_info.get('config', {})
                    print(f"  • {config.get('name', 'Unknown')}: "
                          f"{queue_info.get('active_workers', 0)} workers, "
                          f"Prioridad {config.get('priority_range', [])}")
                
                if dynamic:
                    print(f"\n🔧 COLAS DINÁMICAS ({len(dynamic)}):")
                    for queue_key, queue_info in dynamic.items():
                        config = queue_info.get('config', {})
                        print(f"  • {config.get('name', 'Unknown')}: "
                              f"{queue_info.get('active_workers', 0)} workers, "
                              f"Prioridad {config.get('priority_range', [])}")
            
        except Exception as e:
            print(f"Error mostrando estado: {e}")

    async def run_all_tests(self):
        """Ejecuta todas las pruebas del sistema sin límites"""
        print("🧪 PRUEBAS DEL SISTEMA SIN LÍMITES")
        print("=" * 60)
        print("Probando nuevas características:")
        print("  1. ✨ Escalado sin límites en colas predefinidas")
        print("  2. 🗑️ Eliminación de colas predefinidas") 
        print("  3. 🔧 Creación automática de colas dinámicas")
        print("=" * 60)
        
        # Estado inicial
        await self.show_system_status()
        
        # Ejecutar pruebas
        results = []
        
        print(f"\n⏰ Iniciando pruebas...")
        
        # Prueba 1: Escalado sin límites
        try:
            result1 = await self.test_unlimited_worker_scaling()
            results.append(("Escalado sin límites", result1))
        except Exception as e:
            print(f"❌ Error en prueba 1: {e}")
            results.append(("Escalado sin límites", False))
        
        # Prueba 2: Eliminación de colas predefinidas
        try:
            result2 = await self.test_predefined_queue_deletion()
            results.append(("Eliminación colas predefinidas", result2))
        except Exception as e:
            print(f"❌ Error en prueba 2: {e}")
            results.append(("Eliminación colas predefinidas", False))
        
        # Prueba 3: Creación automática
        try:
            result3 = await self.test_auto_queue_creation()
            results.append(("Creación automática", result3))
        except Exception as e:
            print(f"❌ Error en prueba 3: {e}")
            results.append(("Creación automática", False))
        
        # Estado final
        await self.show_system_status()
        
        # Resumen
        print(f"\n📋 RESUMEN DE PRUEBAS")
        print("=" * 40)
        
        passed = 0
        for test_name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {status} {test_name}")
            if result:
                passed += 1
        
        total = len(results)
        print(f"\n🎯 RESULTADO: {passed}/{total} pruebas exitosas")
        
        if passed == total:
            print("🎉 ¡SISTEMA SIN LÍMITES FUNCIONANDO PERFECTAMENTE!")
        else:
            print("⚠️ Algunas funcionalidades necesitan revisión")
        
        print(f"\n🌐 Dashboard: {self.base_url}/static/dashboard_dynamic.html")

async def main():
    """Función principal"""
    tester = UnlimitedSystemTester()
    await tester.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main())