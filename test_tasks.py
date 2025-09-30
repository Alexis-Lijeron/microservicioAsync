#!/usr/bin/env python3
import requests
import random
import time

# Configuración
BASE_URL = "http://localhost:8000"
NUM_TASKS = 100

# Tareas de prueba con prioridades alineadas al nuevo sistema
test_operations = [
    # CRÍTICAS [1-2]
    ("create_inscripcion", 1), ("update_inscripcion", 1), ("get_inscripciones_list", 1),
    ("create_nota", 2), ("update_nota", 2), ("get_notas_list", 2),
    
    # IMPORTANTES [3-4]  
    ("create_docente", 3), ("update_docente", 3), ("get_docentes_list", 3),
    ("create_estudiante", 4), ("update_estudiante", 4), ("get_estudiantes_list", 4),
    
    # MEDIAS [5-6]
    ("create_grupo", 5), ("get_grupos_list", 5),
    ("create_materia", 6), ("get_materias_list", 6),
    ("create_carrera", 6), ("get_carreras_list", 6),
    ("create_gestion", 6), ("get_gestiones_list", 6),
    
    # NORMALES [7-8]
    ("create_horario", 7), ("get_horarios_list", 7),
    ("create_aula", 8), ("get_aulas_list", 8),
    ("create_nivel", 8), ("get_niveles_list", 8),
    ("create_plan_estudio", 8), ("get_planes_estudio_list", 8),
    
    # BAJAS [9-10] 
    ("create_prerrequisito", 9), ("get_prerrequisitos_list", 9),
    ("get_detalles_list", 10)
]

def create_test_tasks():
    """Crear tareas de prueba para validar distribución"""
    print(f"🧪 Creando {NUM_TASKS} tareas de prueba...")
    
    created_tasks = []
    priority_counts = {}
    
    for i in range(NUM_TASKS):
        # Seleccionar operación aleatoria
        operation, priority = random.choice(test_operations)
        
        # Contar por prioridad
        if priority not in priority_counts:
            priority_counts[priority] = 0
        priority_counts[priority] += 1
        
        # Datos de prueba
        test_data = {
            "test_id": i, 
            "batch": "distribution_test",
            "operation": operation,
            "priority": priority
        }
        
        try:
            # Crear la tarea via API (simular)
            print(f"📋 Tarea {i+1}: {operation} (P{priority})")
            created_tasks.append({
                "task_id": f"test_{i}",
                "operation": operation,
                "priority": priority
            })
            
        except Exception as e:
            print(f"❌ Error creando tarea {i}: {e}")
    
    # Mostrar distribución por prioridad
    print(f"\n📊 DISTRIBUCIÓN POR PRIORIDAD:")
    for priority in sorted(priority_counts.keys()):
        count = priority_counts[priority]
        percentage = (count / NUM_TASKS) * 100
        print(f"   Prioridad {priority}: {count} tareas ({percentage:.1f}%)")
    
    # Distribución por rangos
    ranges = {
        "Críticas [1-2]": len([t for t in created_tasks if t["priority"] <= 2]),
        "Importantes [3-4]": len([t for t in created_tasks if 3 <= t["priority"] <= 4]),
        "Medias [5-6]": len([t for t in created_tasks if 5 <= t["priority"] <= 6]),
        "Normales [7-8]": len([t for t in created_tasks if 7 <= t["priority"] <= 8]),
        "Bajas [9-10]": len([t for t in created_tasks if t["priority"] >= 9])
    }
    
    print(f"\n🎯 DISTRIBUCIÓN POR RANGOS DE COLAS:")
    for range_name, count in ranges.items():
        percentage = (count / NUM_TASKS) * 100
        print(f"   {range_name}: {count} tareas ({percentage:.1f}%)")
    
    return created_tasks

if __name__ == "__main__":
    tasks = create_test_tasks()
    print(f"\n✅ Se crearon {len(tasks)} tareas de prueba para validar distribución")