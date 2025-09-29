import asyncio
from datetime import time, date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config.database import async_session_factory
from app.core.security import get_password_hash
from app.models.carrera import Carrera
from app.models.plan_estudio import PlanEstudio
from app.models.nivel import Nivel
from app.models.materia import Materia
from app.models.prerrequisito import Prerrequisito
from app.models.docente import Docente
from app.models.estudiante import Estudiante
from app.models.aula import Aula
from app.models.horario import Horario
from app.models.gestion import Gestion
from app.models.grupo import Grupo
from app.models.inscripcion import Inscripcion
from app.models.nota import Nota
from app.models.detalle import Detalle


async def seed_database():
    """Poblar la base de datos con datos iniciales"""
    
    async with async_session_factory() as db:
        try:
            print("🌱 Iniciando seeding de la base de datos...")
            
            # 1. Crear Carrera
            print("🎓 Creando carrera...")
            carrera = Carrera(
                codigo='INF187',
                nombre='Ingeniería Informática'
            )
            db.add(carrera)
            await db.commit()
            await db.refresh(carrera)
            
            # 2. Crear Plan de Estudios
            print("📚 Creando plan de estudios...")
            plan_estudio = PlanEstudio(
                codigo='PLAN-INF187',
                cant_semestre=10,
                plan='187-3',
                carrera_codigo=carrera.codigo
            )
            db.add(plan_estudio)
            await db.commit()
            await db.refresh(plan_estudio)
            
            # 3. Crear Niveles (1-10 semestres)
            print("📊 Creando niveles...")
            niveles = []
            for i in range(1, 11):
                nivel = Nivel(
                    codigo_nivel=f'NIV{i:02d}',
                    nivel=i
                )
                db.add(nivel)
                niveles.append(nivel)
            await db.commit()
            
            # Refresh niveles para obtener códigos
            for nivel in niveles:
                await db.refresh(nivel)
            
            # 4. Crear Materias por semestre
            print("📖 Creando materias...")
            materias_data = [
                # SEM 1
                {'sigla': 'MAT101', 'nombre': 'Cálculo I', 'creditos': 5, 'nivel': 1},
                {'sigla': 'INF119', 'nombre': 'Estructuras Discretas', 'creditos': 4, 'nivel': 1},
                {'sigla': 'INF110', 'nombre': 'Introducción a la Informática', 'creditos': 3, 'nivel': 1},
                {'sigla': 'FIS100', 'nombre': 'Física I', 'creditos': 4, 'nivel': 1},
                {'sigla': 'LIN100', 'nombre': 'Inglés Técnico I', 'creditos': 2, 'nivel': 1},
                
                # SEM 2
                {'sigla': 'MAT102', 'nombre': 'Cálculo II', 'creditos': 5, 'nivel': 2},
                {'sigla': 'MAT103', 'nombre': 'Álgebra Lineal', 'creditos': 4, 'nivel': 2},
                {'sigla': 'INF120', 'nombre': 'Programación I', 'creditos': 4, 'nivel': 2},
                {'sigla': 'FIS102', 'nombre': 'Física II', 'creditos': 4, 'nivel': 2},
                {'sigla': 'LIN101', 'nombre': 'Inglés Técnico II', 'creditos': 2, 'nivel': 2},
                
                # SEM 3
                {'sigla': 'MAT207', 'nombre': 'Ecuaciones Diferenciales', 'creditos': 4, 'nivel': 3},
                {'sigla': 'INF210', 'nombre': 'Programación II', 'creditos': 4, 'nivel': 3},
                {'sigla': 'INF211', 'nombre': 'Arquitectura de Computadoras', 'creditos': 4, 'nivel': 3},
                {'sigla': 'FIS200', 'nombre': 'Física III', 'creditos': 4, 'nivel': 3},
                {'sigla': 'ADM100', 'nombre': 'Administración', 'creditos': 3, 'nivel': 3},
                
                # SEM 4
                {'sigla': 'MAT202', 'nombre': 'Probabilidades y Estadísticas I', 'creditos': 4, 'nivel': 4},
                {'sigla': 'INF205', 'nombre': 'Métodos Numéricos', 'creditos': 4, 'nivel': 4},
                {'sigla': 'INF220', 'nombre': 'Estructura de Datos I', 'creditos': 4, 'nivel': 4},
                {'sigla': 'INF221', 'nombre': 'Programación Ensamblador', 'creditos': 4, 'nivel': 4},
                {'sigla': 'ADM200', 'nombre': 'Contabilidad', 'creditos': 3, 'nivel': 4},
                
                # SEM 5
                {'sigla': 'MAT302', 'nombre': 'Probabilidades y Estadísticas II', 'creditos': 4, 'nivel': 5},
                {'sigla': 'INF318', 'nombre': 'Programación Lógica y Funcional', 'creditos': 4, 'nivel': 5},
                {'sigla': 'INF310', 'nombre': 'Estructura de Datos II', 'creditos': 4, 'nivel': 5},
                {'sigla': 'INF312', 'nombre': 'Base de Datos I', 'creditos': 4, 'nivel': 5},
                {'sigla': 'INF319', 'nombre': 'Lenguajes Formales', 'creditos': 4, 'nivel': 5},
                
                # SEM 6
                {'sigla': 'MAT329', 'nombre': 'Investigación Operativa I', 'creditos': 4, 'nivel': 6},
                {'sigla': 'INF342', 'nombre': 'Sistemas de Información I', 'creditos': 4, 'nivel': 6},
                {'sigla': 'INF323', 'nombre': 'Sistemas Operativos I', 'creditos': 4, 'nivel': 6},
                {'sigla': 'INF322', 'nombre': 'Base de Datos II', 'creditos': 4, 'nivel': 6},
                {'sigla': 'INF329', 'nombre': 'Compiladores', 'creditos': 4, 'nivel': 6},
                
                # SEM 7
                {'sigla': 'MAT419', 'nombre': 'Investigación Operativa II', 'creditos': 4, 'nivel': 7},
                {'sigla': 'INF418', 'nombre': 'Inteligencia Artificial', 'creditos': 4, 'nivel': 7},
                {'sigla': 'INF413', 'nombre': 'Sistemas Operativos II', 'creditos': 4, 'nivel': 7},
                {'sigla': 'INF433', 'nombre': 'Redes I', 'creditos': 4, 'nivel': 7},
                {'sigla': 'INF412', 'nombre': 'Sistemas de Información II', 'creditos': 4, 'nivel': 7},
                
                # SEM 8
                {'sigla': 'ECO449', 'nombre': 'Preparación y Evaluación de Proyectos', 'creditos': 3, 'nivel': 8},
                {'sigla': 'INF428', 'nombre': 'Sistemas Expertos', 'creditos': 4, 'nivel': 8},
                {'sigla': 'INF442', 'nombre': 'Sistemas de Información Geográfica', 'creditos': 4, 'nivel': 8},
                {'sigla': 'INF423', 'nombre': 'Redes II', 'creditos': 4, 'nivel': 8},
                {'sigla': 'INF422', 'nombre': 'Ingeniería de Software I', 'creditos': 4, 'nivel': 8},
                
                # SEM 9
                {'sigla': 'INF511', 'nombre': 'Taller de Grado I', 'creditos': 3, 'nivel': 9},
                {'sigla': 'INF512', 'nombre': 'Ingeniería de Software II', 'creditos': 4, 'nivel': 9},
                {'sigla': 'INF513', 'nombre': 'Tecnología Web', 'creditos': 4, 'nivel': 9},
                {'sigla': 'INF552', 'nombre': 'Arquitectura del Software', 'creditos': 4, 'nivel': 9},
                
                # SEM 10
                {'sigla': 'GRL001', 'nombre': 'Modalidad de Titulación / Licenciatura', 'creditos': 0, 'nivel': 10},
                
                # Electivas
                {'sigla': 'ELC101', 'nombre': 'Modelación y Simulación de Sistemas', 'creditos': 4, 'nivel': 9, 'electiva': True},
                {'sigla': 'ELC102', 'nombre': 'Programación Gráfica', 'creditos': 4, 'nivel': 9, 'electiva': True},
                {'sigla': 'ELC103', 'nombre': 'Tópicos Avanzados de Programación', 'creditos': 4, 'nivel': 9, 'electiva': True},
                {'sigla': 'ELC104', 'nombre': 'Programación de Aplicaciones de Tiempo Real', 'creditos': 4, 'nivel': 9, 'electiva': True},
                {'sigla': 'ELC105', 'nombre': 'Sistemas Distribuidos', 'creditos': 4, 'nivel': 9, 'electiva': True},
                {'sigla': 'ELC106', 'nombre': 'Interacción Hombre-Computador', 'creditos': 4, 'nivel': 9, 'electiva': True},
                {'sigla': 'ELC107', 'nombre': 'Criptografía y Seguridad', 'creditos': 4, 'nivel': 9, 'electiva': True},
                {'sigla': 'ELC108', 'nombre': 'Control y Automatización', 'creditos': 4, 'nivel': 9, 'electiva': True},
            ]
            
            materias_dict = {}
            for mat_data in materias_data:
                nivel_idx = mat_data['nivel'] - 1
                materia = Materia(
                    sigla=mat_data['sigla'],
                    nombre=mat_data['nombre'],
                    creditos=mat_data['creditos'],
                    es_electiva=mat_data.get('electiva', False),
                    nivel_codigo=niveles[nivel_idx].codigo_nivel,
                    plan_estudio_codigo=plan_estudio.codigo
                )
                db.add(materia)
                materias_dict[mat_data['sigla']] = materia
            
            await db.commit()
            
            # Refresh materias
            for materia in materias_dict.values():
                await db.refresh(materia)
            
            # 5. Crear Prerrequisitos
            print("🔗 Creando prerrequisitos...")
            prerrequisitos = [
                ('MAT101', 'MAT102'),
                ('MAT102', 'MAT207'),
                ('MAT103', 'MAT207'),
                ('FIS100', 'FIS102'),
                ('LIN100', 'LIN101'),
                ('INF120', 'INF210'),
                ('INF120', 'INF211'),
                ('INF210', 'INF220'),
                ('INF210', 'INF221'),
                ('MAT207', 'INF205'),
                ('MAT207', 'MAT202'),
                ('MAT202', 'MAT302'),
                ('INF220', 'INF310'),
                ('INF220', 'INF312'),
                ('INF119', 'INF319'),
                ('INF312', 'INF322'),
                ('INF310', 'INF323'),
                ('INF319', 'INF329'),
                ('MAT302', 'MAT329'),
                ('INF312', 'INF342'),
                ('MAT329', 'MAT419'),
                ('INF329', 'INF418'),
                ('INF323', 'INF413'),
                ('INF322', 'INF433'),
                ('INF342', 'INF412'),
                ('INF418', 'INF428'),
                ('INF433', 'INF423'),
                ('INF433', 'INF442'),
                ('INF342', 'INF422'),
                ('INF422', 'INF512'),
                ('INF423', 'INF513'),
                ('INF512', 'INF552'),
                ('INF511', 'GRL001'),
            ]
            
            for sigla_pre, sigla_materia in prerrequisitos:
                if sigla_materia in materias_dict:
                    prerrequisito = Prerrequisito(
                        codigo_prerrequisito=f'PRE-{sigla_materia}-{sigla_pre}',
                        materia_sigla=materias_dict[sigla_materia].sigla,
                        sigla_prerrequisito=sigla_pre
                    )
                    db.add(prerrequisito)
            
            await db.commit()
            
            # 6. Crear Docentes
            print("👨‍🏫 Creando docentes...")
            docentes_data = [
                ('María', 'Gutiérrez'),
                ('Juan', 'Ramírez'),
                ('Ana', 'Paredes'),
                ('Luis', 'Mendoza'),
                ('Carla', 'Rojas'),
                ('Diego', 'Cabrera')
            ]
            
            docentes = []
            for i, (nombre, apellido) in enumerate(docentes_data, 1):
                docente = Docente(
                    codigo_docente=f'DOC{i:03d}',
                    nombre=nombre, 
                    apellido=apellido
                )
                db.add(docente)
                docentes.append(docente)
            
            await db.commit()
            
            # Refresh docentes
            for docente in docentes:
                await db.refresh(docente)
            
            # 7. Crear Estudiantes
            print("👨‍🎓 Creando estudiantes...")
            estudiantes_data = [
                ('Victor', 'Salvatierra', 'VIC001', '12345671'),
                ('Tatiana', 'Cuéllar', 'TAT002', '12345672'),
                ('Gabriel', 'Fernández', 'GAB003', '12345673'),
                ('Lucía', 'Soto', 'LUC004', '12345674'),
                ('Álvaro', 'Pérez', 'ALV005', '12345675'),
                ('Sofía', 'Ribas', 'SOF006', '12345676'),
                ('Daniel', 'Flores', 'DAN007', '12345677'),
                ('Carolina', 'López', 'CAR008', '12345678'),
            ]
            
            estudiantes = []
            for nombre, apellido, registro, ci in estudiantes_data:
                estudiante = Estudiante(
                    registro=registro,
                    nombre=nombre,
                    apellido=apellido,
                    ci=ci,
                    contraseña=get_password_hash('123456'),  # Password por defecto
                    carrera_codigo=carrera.codigo
                )
                db.add(estudiante)
                estudiantes.append(estudiante)
            
            await db.commit()
            
            # Refresh estudiantes
            for estudiante in estudiantes:
                await db.refresh(estudiante)
            
            # 8. Crear Aulas
            print("🏫 Creando aulas...")
            aulas_data = [
                ('236', '10'), ('236', '12'), ('236', '13'),
                ('236', '21'), ('236', '22'), ('236', '31')
            ]
            
            aulas = []
            for i, (modulo, aula_num) in enumerate(aulas_data, 1):
                aula = Aula(
                    codigo_aula=f'AUL{modulo}-{aula_num}',
                    modulo=modulo, 
                    aula=aula_num
                )
                db.add(aula)
                aulas.append(aula)
            
            await db.commit()
            
            # Refresh aulas
            for aula in aulas:
                await db.refresh(aula)
            
            # 9. Crear Horarios
            print("⏰ Creando horarios...")
            horarios_data = [
                ('Lunes', time(8, 0), time(10, 0), aulas[0].codigo_aula),
                ('Martes', time(10, 0), time(12, 0), aulas[1].codigo_aula),
                ('Miércoles', time(14, 0), time(16, 0), aulas[2].codigo_aula),
                ('Jueves', time(16, 0), time(18, 0), aulas[3].codigo_aula),
                ('Viernes', time(8, 0), time(10, 0), aulas[4].codigo_aula),
            ]
            
            horarios = []
            for i, (dia, hora_inicio, hora_final, aula_codigo) in enumerate(horarios_data, 1):
                horario = Horario(
                    codigo_horario=f'HOR{i:03d}',
                    dia=dia,
                    hora_inicio=hora_inicio,
                    hora_final=hora_final,
                    aula_codigo=aula_codigo
                )
                db.add(horario)
                horarios.append(horario)
            
            await db.commit()
            
            # Refresh horarios
            for horario in horarios:
                await db.refresh(horario)
            
            # 10. Crear Gestiones
            print("📅 Creando gestiones...")
            gestiones_data = [
                (1, 2025), (2, 2025), (3, 2025), (4, 2025)
            ]
            
            gestiones = []
            for i, (semestre, año) in enumerate(gestiones_data, 1):
                gestion = Gestion(
                    codigo_gestion=f'GEST{año}-{semestre}',
                    semestre=semestre, 
                    año=año
                )
                db.add(gestion)
                gestiones.append(gestion)
            
            await db.commit()
            
            # Refresh gestiones
            for gestion in gestiones:
                await db.refresh(gestion)
            
            # 11. Crear Grupos
            print("👥 Creando grupos...")
            grupos_data = [
                ('INF120-01 Programación I - SEM 2/2025', docentes[0].codigo_docente, gestiones[1].codigo_gestion, 'INF120', horarios[0].codigo_horario),
                ('MAT101-01 Cálculo I - SEM 1/2025', docentes[1].codigo_docente, gestiones[0].codigo_gestion, 'MAT101', horarios[1].codigo_horario),
                ('INF312-01 Base de Datos I - SEM 2/2025', docentes[2].codigo_docente, gestiones[1].codigo_gestion, 'INF312', horarios[2].codigo_horario),
                ('INF323-01 Sistemas Operativos I - SEM 2/2025', docentes[3].codigo_docente, gestiones[1].codigo_gestion, 'INF323', horarios[3].codigo_horario),
                ('INF433-01 Redes I - SEM 2/2025', docentes[4].codigo_docente, gestiones[1].codigo_gestion, 'INF433', horarios[4].codigo_horario),
            ]
            
            grupos = []
            for i, (descripcion, docente_codigo, gestion_codigo, sigla_materia, horario_codigo) in enumerate(grupos_data, 1):
                if sigla_materia in materias_dict:
                    grupo = Grupo(
                        codigo_grupo=f'GRP{i:03d}',
                        descripcion=descripcion,
                        docente_codigo=docente_codigo,
                        gestion_codigo=gestion_codigo,
                        materia_sigla=materias_dict[sigla_materia].sigla,
                        horario_codigo=horario_codigo
                    )
                    db.add(grupo)
                    grupos.append(grupo)
            
            await db.commit()
            
            # Refresh grupos
            for grupo in grupos:
                await db.refresh(grupo)
            
            # 12. Crear Inscripciones
            print("📝 Creando inscripciones...")
            inscripciones_data = [
                (1, gestiones[0].codigo_gestion, estudiantes[0].registro, grupos[0].codigo_grupo),
                (1, gestiones[0].codigo_gestion, estudiantes[1].registro, grupos[0].codigo_grupo),
                (1, gestiones[0].codigo_gestion, estudiantes[2].registro, grupos[0].codigo_grupo),
            ]
            
            for i, (semestre, gestion_codigo, estudiante_registro, grupo_codigo) in enumerate(inscripciones_data, 1):
                inscripcion = Inscripcion(
                    codigo_inscripcion=f'INS{i:03d}',
                    semestre=semestre,
                    gestion_codigo=gestion_codigo,
                    estudiante_registro=estudiante_registro,
                    grupo_codigo=grupo_codigo
                )
                db.add(inscripcion)
            
            await db.commit()
            
            # 13. Crear Notas
            print("📊 Creando notas...")
            notas_data = [
                (78.50, estudiantes[0].registro),
                (65.00, estudiantes[1].registro),
                (91.00, estudiantes[2].registro),
                (84.25, estudiantes[0].registro),
                (72.00, estudiantes[1].registro),
                (88.00, estudiantes[0].registro),
                (74.50, estudiantes[4].registro),
                (69.00, estudiantes[5].registro),
                (81.00, estudiantes[0].registro),
                (70.00, estudiantes[6].registro),
                (79.00, estudiantes[0].registro),
                (86.50, estudiantes[1].registro),
                (60.00, estudiantes[7].registro),
            ]
            
            for i, (nota_valor, estudiante_registro) in enumerate(notas_data, 1):
                nota = Nota(
                    codigo_nota=f'NOT{i:03d}',
                    nota=nota_valor, 
                    estudiante_registro=estudiante_registro
                )
                db.add(nota)
            
            await db.commit()
            
            # 14. Crear Detalles
            print("📋 Creando detalles...")
            detalles_data = [
                (date(2025, 3, 10), time(8, 0), grupos[0].codigo_grupo),
                (date(2025, 3, 17), time(8, 0), grupos[0].codigo_grupo),
                (date(2025, 2, 20), time(10, 0), grupos[1].codigo_grupo),
                (date(2025, 5, 5), time(14, 0), grupos[2].codigo_grupo),
                (date(2025, 4, 12), time(16, 0), grupos[3].codigo_grupo),
                (date(2025, 6, 1), time(8, 0), grupos[4].codigo_grupo),
            ]
            
            for i, (fecha, hora, grupo_codigo) in enumerate(detalles_data, 1):
                detalle = Detalle(
                    codigo_detalle=f'DET{i:03d}',
                    fecha=fecha, 
                    hora=hora, 
                    grupo_codigo=grupo_codigo
                )
                db.add(detalle)
            
            await db.commit()
            
            print("✅ Seeding completado exitosamente!")
            print(f"🎓 Carrera creada: {carrera.nombre} (Código: {carrera.codigo})")
            print(f"📚 Plan de estudios: {plan_estudio.plan} (Código: {plan_estudio.codigo})")
            print(f"📖 Materias creadas: {len(materias_data)}")
            print(f"👨‍🏫 Docentes creados: {len(docentes)}")
            print(f"👨‍🎓 Estudiantes creados: {len(estudiantes)}")
            print(f"👥 Grupos creados: {len(grupos)}")
            print("\n🔐 Credenciales de estudiantes:")
            for est in estudiantes:
                print(f"   📋 {est.registro} / 123456 ({est.nombre} {est.apellido})")
            
        except Exception as e:
            print(f"❌ Error durante seeding: {e}")
            await db.rollback()
            raise


async def check_if_seeded(db: AsyncSession) -> bool:
    """Verificar si la base de datos ya tiene datos"""
    result = await db.execute(select(Carrera))
    carreras = result.scalars().all()
    return len(carreras) > 0


async def run_seeder():
    """Ejecutar seeder solo si no hay datos"""
    async with async_session_factory() as db:
        if await check_if_seeded(db):
            print("📊 Base de datos ya tiene datos, saltando seeding...")
            return False
        
    await seed_database()
    return True