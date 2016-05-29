CREATE VOLATILE TABLE PASO1, NO LOG AS(
select G.*,
trim(SUBSTR(FECHA_1, 7, 4) ) || trim(SUBSTR(FECHA_1, 4, 2) )   || trim(SUBSTR(FECHA_1, 1, 2) )  Fec
from FIDELIZACION.PFA_DG_BO g
) WITH DATA
ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE PASO2, NO LOG AS(
select *
from paso1
where observacion <> '.'
qualify (row_number() over(partition by num_celular order by Fec desc ))= 1
) WITH DATA
ON COMMIT PRESERVE ROWS;


CREATE VOLATILE TABLE PASO3, NO LOG AS(
select a.Num_abonado,
			b.num_celular,
			a.plan Plan_Anteior,
			b.plan Plan_Actual,
			E.CB_ACTUAL VALOR_ANTERIOR,
			F.CB_ACTUAL VALOR_ACTUAL,
			Observacion,
			CASE
			WHEN (F.CB_ACTUAL  - E.CB_ACTUAL)  > 0 THEN 'UPGRADE'
			WHEN F.CB_ACTUAL  = E.CB_ACTUAL  THEN 'SIN CAMBIO'
			WHEN (F.CB_ACTUAL  - E.CB_ACTUAL)  < 0 THEN 'DOWNGRADE'
			END MOVIMIENTO,
			(F.CB_ACTUAL  - E.CB_ACTUAL) DIFERENCIA,
			CASE
			WHEN   ((F.CB_ACTUAL + 32350 ) - E.CB_ACTUAL)  >= 0 THEN 'BAJA SERVICIO PLAN FAMILIA Y AMIGOS'
			WHEN   ((F.CB_ACTUAL + 32350 ) - E.CB_ACTUAL)  < 0 THEN 'LLAMAR ABONADOS'
			ELSE 'VERIFICAR'
			END IDENTIFICADOR
from FIDELIZACION.PFA_DG_PL_Anterior  a
left join FIDELIZACION.PFA_DG_PL_Actual b on a.Num_Abonado = b.Num_Abonado
left join FIDELIZACION.PFA_DG_PL_INN c on a.plan = c.plan
left join FIDELIZACION.PFA_DG_PL_INN d on b.plan = d.plan
left join FIDELIZACION.CP_VALOR_PLANES E on A.plan = E.COD_PLAN
left join FIDELIZACION.CP_VALOR_PLANES F on B.plan = F.COD_PLAN
left join Paso2 g on trim(b.num_celular) = trim(g.num_celular)
where a.plan <> b.plan 
and  F.CB_ACTUAL  < E.CB_ACTUAL
and c.plan is not null
and d.plan is null
) WITH DATA
ON COMMIT PRESERVE ROWS;


select *
from PASO3 
where observacion not like '%DESACTIVAR%'
and identificador = 'BAJA SERVICIO PLAN FAMILIA Y AMIGOS'
and movimiento = 'DOWNGRADE'
