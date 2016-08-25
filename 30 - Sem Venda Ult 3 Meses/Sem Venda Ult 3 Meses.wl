(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31,9999};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getALIMENTUM[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
	
		SET NOCOUNT ON;	
	    SELECT DISTINCT COD_LOJA, COD_PRODUTO INTO #TMP_VENDAS FROM BI.DBO.BI_VENDA_PRODUTO WITH (NOLOCK) WHERE 1=1 AND DATA >= CONVERT(DATE,GETDATE()-90)

SELECT COD_LOJA, COD_PRODUTO, CAST(QTD_ESTOQUE  AS DOUBLE PRECISION) AS QTD_ESTOQUE INTO  #TMP_ESTOQUE  FROM DW.DBO.ESTOQUE WHERE 1=1 
AND DATA =  CONVERT(DATE,GETDATE()) 
AND QTD_ESTOQUE > 0
AND COD_LOJA IN (`1`)

SELECT
A.COD_LOJA
,NO_LOJA
,A.COD_PRODUTO
,DESCRICAO
,NO_DEPARTAMENTO	
,QTD_ESTOQUE
FROM #TMP_ESTOQUE A 
LEFT JOIN #TMP_VENDAS B ON B.COD_LOJA = A.COD_LOJA AND B.COD_PRODUTO = A.COD_PRODUTO 
LEFT JOIN  BI.DBO.BI_CAD_PRODUTO C ON C.COD_PRODUTO = A.COD_PRODUTO
LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2]  d on d.COD_LOJA = a.COD_LOJA
WHERE 1=1
AND B.COD_PRODUTO IS NULL
AND COD_DEPARTAMENTO NOT IN (19,5,15,7,20,17,99,16,12,21,14)
ORDER BY QTD_ESTOQUE DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r\[LeftArrow]getALIMENTUM[$lojas]


getTOP23[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

	SET NOCOUNT ON;	
		
SELECT DISTINCT COD_LOJA, COD_PRODUTO INTO #TMP_VENDAS FROM BI.DBO.BI_VENDA_PRODUTO WITH (NOLOCK) WHERE 1=1 AND DATA >= CONVERT(DATE,GETDATE()-90)

SELECT COD_LOJA, COD_PRODUTO, CAST(QTD_ESTOQUE  AS DOUBLE PRECISION) AS QTD_ESTOQUE INTO  #TMP_ESTOQUE  FROM DW.DBO.ESTOQUE WHERE 1=1 
AND DATA =  CONVERT(DATE,GETDATE()) 
AND QTD_ESTOQUE > 0
AND COD_LOJA IN (`1`)

SELECT TOP 22
NO_LOJA Loja
,A.COD_PRODUTO Plu
,DESCRICAO as 'Descri\[CCedilla]\[ATilde]o'
,NO_DEPARTAMENTO Dep
,QTD_ESTOQUE Estoque
FROM #TMP_ESTOQUE A 
LEFT JOIN #TMP_VENDAS B ON B.COD_LOJA = A.COD_LOJA AND B.COD_PRODUTO = A.COD_PRODUTO 
LEFT JOIN  BI.DBO.BI_CAD_PRODUTO C ON C.COD_PRODUTO = A.COD_PRODUTO
LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2]  d on d.COD_LOJA = a.COD_LOJA
WHERE 1=1
AND B.COD_PRODUTO IS NULL
AND COD_DEPARTAMENTO NOT IN (19,5,15,7,20,17,99,16,12,21,14)
ORDER BY QTD_ESTOQUE DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r2\[LeftArrow]getTOP23[$lojas]


grid8[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<0,Red
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Estoque"}-> stl},"TotalRow"->False];
	title=Style[Row@{"Top Itens"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid8[r2];


getTOTAL[codLoja_]:=Module[{sql,s,conn=marcheConn2[],tab},
	sql=" 

			SET NOCOUNT ON;	

			SELECT DISTINCT COD_LOJA, COD_PRODUTO INTO #TMP_VENDAS FROM BI.DBO.BI_VENDA_PRODUTO WITH (NOLOCK) WHERE 1=1 AND DATA >= CONVERT(DATE,GETDATE()-90)

			SELECT COD_LOJA, COD_PRODUTO, CAST(QTD_ESTOQUE  AS DOUBLE PRECISION) AS QTD_ESTOQUE INTO  #TMP_ESTOQUE  FROM DW.DBO.ESTOQUE WHERE 1=1 
					AND DATA =  CONVERT(DATE,GETDATE()) 
					AND QTD_ESTOQUE > 0
					AND COD_LOJA IN (`1`)

			SELECT
					a.Cod_loja
					,SUM(CASE WHEN QTD_ESTOQUE >= 10 THEN 1 ELSE 0 END ) 'Estoque > 10'
					,SUM(CASE WHEN QTD_ESTOQUE >= 100 THEN 1 ELSE 0 END ) 'Estoque > 100'
					,COUNT(QTD_ESTOQUE) Total	
			into #tmp_resumo
			FROM #TMP_ESTOQUE A 
			LEFT JOIN #TMP_VENDAS B ON B.COD_LOJA = A.COD_LOJA AND B.COD_PRODUTO = A.COD_PRODUTO 
			LEFT JOIN  BI.DBO.BI_CAD_PRODUTO C ON C.COD_PRODUTO = A.COD_PRODUTO
			LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2]  d on d.COD_LOJA = a.COD_LOJA

			WHERE 1=1
			AND B.COD_PRODUTO IS NULL
			AND COD_DEPARTAMENTO NOT IN (19,5,15,7,20,17,99,16,12,21,14)
			GROUP BY 
			a.Cod_loja


			SELECT 
					A.COD_LOJA
					,CASE A.COD_LOJA WHEN 4 THEN 'Cotoxo' WHEN 10 THEN 'SCS' WHEN 19 THEN 'Sumare' WHEN 28 THEN 'Orbis' ELSE [NO_LOJA] END AS Loja
					,ISNULL(SUM(B.[Estoque > 10]),0) 'Estoque > 10' 
					,ISNULL(SUM(B.[Estoque > 100]),0) 'Estoque > 100' 
					,ISNULL(SUM(B.Total),0) Total 
					INTO #TOTAL_TMP
			FROM 
					[BI].[DBO].[BI_CAD_LOJA2] AS A LEFT JOIN #tmp_resumo AS B ON B.COD_LOJA = A.COD_LOJA
			WHERE 1=1 
					AND A.COD_LOJA  IN (`1`)
			GROUP BY a.COD_LOJA,NO_LOJA

			UNION
			SELECT 9999 as COD_LOJA
			,'Total' as 'Total' 
			,ISNULL(SUM([Estoque > 10]),0) 'Estoque > 10' 
			,ISNULL(SUM([Estoque > 100]),0) 'Estoque > 100' 
			,ISNULL(SUM(Total),0) Total 
			FROM #TMP_RESUMO
			WHERE 1=1 
				 AND COD_LOJA  IN (`1`)
			ORDER BY COD_LOJA


			SELECT Loja, [Estoque > 10],[Estoque > 100] ,Total FROM #TOTAL_TMP
			WHERE 1=1 
			     AND COD_LOJA IN (`1`)	   
 ";
		s\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
		s
];
s\[LeftArrow]getTOTAL[$lojas]


gridTOTAL[data_Symbol]:=Module[{grid,title,s,color2,stl},
	s\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[s,"ColumnFormat"->{{"Estoque > 100","Total"}-> stl},"TotalRow"->True];
	title=Style[Row@{"Por Loja"},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridTOTAL[s];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridTOTAL[s],grid8[r2]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];
rep=marcheTemplate[grid=Row@{"Produtos Obsoletos ",$now},createReport[s],1200, $logo];


$fileName1="Auditoria Obsoletos "<>$now2<>".png";
Export["Auditoria Obsoletos "<>$now2<>".png",rep];


$fileName="Rela\[CCedilla]\[ATilde]o de Produtos "<>$now2<>".xlsx";
Export["Rela\[CCedilla]\[ATilde]o de Produtos "<>$now2<>".xlsx",r["DataAll"]];		


marcheMail[
  		"Produtos Obsoletos " <>$now 
  		,"Produtos Obsoletos " <>$now 
		  ,$mailsGerencia
		  ,{$fileName1,$fileName}
]
