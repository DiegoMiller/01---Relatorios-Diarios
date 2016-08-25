(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$lojas={1,2,3,6,7,8,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31,33,9999};

$CodProdutosDesconsiderados = {1};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getRESUMO[CodProdutosDesconsiderados_]:=Module[{sql,r,conn=marcheConn2[],tab},

sql=" 
	
SELECT DISTINCT TOP 10
A.COD_PRODUTO Plu
,DESCRICAO as Produto
,NO_DEPARTAMENTO Dep
,convert(varchar,DTA_Gravacao,103) [Dta Solicita\[CCedilla]\[ATilde]o]
,DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) Dias
,CASE 	
	WHEN DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) <=1  THEN 'Baixo'
	WHEN DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) =2  THEN 'Medio'
ELSE 'Alto'
END AS [Nivel Aten\[CCedilla]\[ATilde]o]
,USUARIO as [Solicitante]
FROM DW.[DBO].[CMV_ANALISE_STATUS] A LEFT JOIN BI.DBO.BI_CAD_PRODUTO B ON B.COD_PRODUTO = A.COD_PRODUTO
	WHERE 1=1 
		AND A.COD_PRODUTO NOT IN (`1`)
		and Dta_Analise is null

ORDER BY DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@CodProdutosDesconsiderados}];
	r
];
r\[LeftArrow]getRESUMO[$CodProdutosDesconsiderados]


gridResumo[data_Symbol]:=Module[{grid,title,r,color2,stl,color3,stl3,colorTest},
	r\[LeftArrow]data;
	color2=Which[ 
			 #>=3,Red
			,True,Darker@Green
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="OK";

	color3=Which[			
			#==="Baixo",Darker@Green
			,#==="Medio",Darker@Blue
			,True,Red
	]&;

	stl3[val_]:= Style[maF["Date"][val],Bold,color3@val];
	stl3[Null]="OK";
	
	grid=maReportGrid[r,"ColumnFormat"->{{"Dias"}-> stl, {"Nivel Aten\[CCedilla]\[ATilde]o"}-> stl3},"TotalRow"->False];
	title=Style[Row@{"Top Solicita\[CCedilla]\[OTilde]es de Analise"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridResumo[r];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridResumo[r]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];
rep=marcheTemplate[grid=Row@{"Status Solicita\[CCedilla]\[OTilde]es ",$now},createReport[s],1200, $logo]


$fileName1="Analise CMV "<>$now2<>".png";
Export["Analise CMV "<>$now2<>".png",rep];


getExcel[CodProdutosDesconsiderados_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
	
SELECT DISTINCT
RECID RecId
,A.COD_PRODUTO Plu
,DESCRICAO as Produto
,NO_DEPARTAMENTO Dep
,convert(date,DTA_Gravacao) [Dta Solicita\[CCedilla]\[ATilde]o]
,DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) Dias
,CASE 	
	WHEN DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) <=1  THEN 'Baixo'
	WHEN DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) =2  THEN 'Medio'
ELSE 'Alto'
END AS [Nivel Aten\[CCedilla]\[ATilde]o]
,USUARIO as [Solicitante]
FROM DW.[DBO].[CMV_ANALISE_STATUS] A LEFT JOIN BI.DBO.BI_CAD_PRODUTO B ON B.COD_PRODUTO = A.COD_PRODUTO
WHERE 1=1 
	and a.cod_produto not in (`1`) 
	and Dta_Analise is null
ORDER BY DATEDIFF (DAY,DTA_GRAVACAO,GETDATE()) DESC
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@CodProdutosDesconsiderados}];
	r
];
r1\[LeftArrow]getExcel[$CodProdutosDesconsiderados];


$fileName="Pendencias de Analise CMV "<>$now2<>".xlsx";
Export["Pendencias de Analise CMV "<>$now2<>".xlsx",r1["DataAll"]];		


getExcelAnalisados[CodProdutosDesconsiderados_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
		
SELECT 
Plu	,Produto	,Dep	,Solicitante	,[Dta Solicita\[CCedilla]\[ATilde]o]	,[Dta Analise]	,Status	,Observacao	
FROM (		
SELECT 
		A.COD_PRODUTO Plu
		,DESCRICAO as Produto
		,NO_DEPARTAMENTO Dep
		,USUARIO Solicitante
		,convert(date,DTA_Gravacao) [Dta Solicita\[CCedilla]\[ATilde]o]
		,RANK() OVER(PARTITION BY A.COD_PRODUTO ORDER BY CONVERT(DATE,Dta_Analise) DESC) AS SEQ
		,convert(date,Dta_Analise) 	[Dta Analise]
		,Status	
		,Observacao
		,Usuario_Analise
		FROM DW.[DBO].[CMV_ANALISE_STATUS] A LEFT JOIN BI.DBO.BI_CAD_PRODUTO B ON B.COD_PRODUTO = A.COD_PRODUTO
	WHERE 1=1 
	AND A.COD_PRODUTO NOT IN (`1`)
	and Dta_Analise is not null

) TMP 
WHERE 1=1 AND SEQ = 1
order by [Dta Analise] desc

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@CodProdutosDesconsiderados}];
	r
];
r5\[LeftArrow]getExcelAnalisados[$CodProdutosDesconsiderados];


$fileName2="Analisados "<>$now2<>".xlsx";
Export["Analisados "<>$now2<>".xlsx",r5["DataAll"]];		


marcheMail[
  		"[GE] Solicita\[CCedilla]\[OTilde]es Analise CMV  " <>$now 
  		,""
		  ,$mailsGerencia
		  ,{$fileName1,$fileName,$fileName2}
]
