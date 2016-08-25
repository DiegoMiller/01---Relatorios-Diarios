(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$now3=DateString[DatePlus[-1],{"Day","-","Month","-","Year"}];
$lojas={33};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getTRAVAS[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 	

SET NOCOUNT ON;	
				
SELECT 
DESC_TRAVA Tipo
,COUNT(COD_FORNECEDOR) [Forn Cad.]
,CONVERT(VARCHAR,MAX(DTA_INCLUSAO),103) [Ult Atualiza\[CCedilla]\[ATilde]o]
FROM DW.DBO.TRAVA_INTRANET A 
		LEFT JOIN DW.DBO.TIPO_TRAVA_INTRANET B 
				ON B.TIPO_TRAVA = A.TIPO_TRAVA
WHERE 1=1
AND FLG_ATIVO = 1
GROUP BY DESC_TRAVA
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r\[LeftArrow]getTRAVAS[$lojas];


gridTRAVAS[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"TotalRow"->False];
	title=Style[Row@{"Status Atualiza\[CCedilla]\[ATilde]o"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridTRAVAS[r];


getTOP15[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
	
	SET NOCOUNT ON;	
			
	SELECT TOP 15
		B.DESC_TRAVA Tipo 
		,A.COD_FORNECEDOR [Cod Forn]
		,DES_FORNECEDOR Fornecedor
		,CONVERT(VARCHAR,MAX(A.DTA_INCLUSAO),103) [Ult Atualiza\[CCedilla]\[ATilde]o]
FROM DW.DBO.TRAVA_INTRANET A 
		LEFT JOIN DW.DBO.TIPO_TRAVA_INTRANET B 
				ON B.TIPO_TRAVA = A.TIPO_TRAVA
		 LEFT JOIN [192.168.0.6].zeus_rtg.dbo.TAB_FORNECEDOR FORN
				ON FORN.COD_FORNECEDOR = A.COD_FORNECEDOR
WHERE 1=1
AND FLG_ATIVO = 1

GROUP BY 
		B.DESC_TRAVA
		,A.COD_FORNECEDOR 
		,DES_FORNECEDOR
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r2\[LeftArrow]getTOP15[$lojas];


grid8[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"TotalRow"->False];
	title=Style[Row@{"Top 15 Fornecedores"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid8[r2];


getEXCEL[codLoja_]:=Module[{sql,s,conn=marcheConn2[],tab},
	sql=" 

			
	SET NOCOUNT ON;	
			
	SELECT 
		B.DESC_TRAVA Tipo
		,A.COD_FORNECEDOR*1 [Cod Forn]
		,DES_FORNECEDOR Fornecedor
		,CONVERT(VARCHAR,MAX(A.DTA_INCLUSAO),103) [Ult Atualiza\[CCedilla]\[ATilde]o]
FROM DW.DBO.TRAVA_INTRANET A 
		LEFT JOIN DW.DBO.TIPO_TRAVA_INTRANET B 
				ON B.TIPO_TRAVA = A.TIPO_TRAVA
		 LEFT JOIN [192.168.0.6].zeus_rtg.dbo.TAB_FORNECEDOR FORN
				ON FORN.COD_FORNECEDOR = A.COD_FORNECEDOR
WHERE 1=1
AND FLG_ATIVO = 1

GROUP BY 
		B.DESC_TRAVA
		,A.COD_FORNECEDOR 
		,DES_FORNECEDOR
 ";
		r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
		r
];
(*s\[LeftArrow]getEXCEL[$lojas];*)


getProdutosEXCEL[codLoja_]:=Module[{sql,s,conn=marcheConn2[],tab},
	sql=" 

	SET NOCOUNT ON;	

	SELECT DISTINCT 
		'TROCAS NF' AS [TIPO TRAVA]
		,A.COD_PRODUTO
		,A.COD_EAN 
		,CADASTRO.DESCRICAO
		,A.COD_FORNECEDOR*1
		,FORN.DES_FORNECEDOR
		
 FROM [192.168.0.6].INTEGRACOES.DBO.FORNECEDOR_PERMITE_TROCA_NF A WITH (NOLOCK)
		 LEFT JOIN BI.DBO.BI_CAD_PRODUTO CADASTRO WITH (NOLOCK)
				ON A.COD_PRODUTO = CADASTRO.COD_PRODUTO
		LEFT JOIN INTEGRACOES.DBO.TAB_FORNECEDOR FORN WITH (NOLOCK)
				ON FORN.COD_FORNECEDOR = A.COD_FORNECEDOR
 
 UNION

SELECT DISTINCT 
		'TROCAS ELAS POR ELAS' AS [TIPO TRAVA]
		,A.COD_PRODUTO
		,A.COD_EAN 
		,CADASTRO.DESCRICAO
		,A.COD_FORNECEDOR
		,FORN.DES_FORNECEDOR
 
 FROM [192.168.0.6].INTEGRACOES.DBO.FORNECEDOR_NAO_PERMITE_TROCA A WITH (NOLOCK)
		 LEFT JOIN BI.DBO.BI_CAD_PRODUTO CADASTRO WITH (NOLOCK)
				ON A.COD_PRODUTO = CADASTRO.COD_PRODUTO
		LEFT JOIN INTEGRACOES.DBO.TAB_FORNECEDOR FORN WITH (NOLOCK)
				ON FORN.COD_FORNECEDOR = A.COD_FORNECEDOR
		 
 UNION
 
SELECT DISTINCT
		'CONSIGNADOS' AS [TIPO TRAVA]
		,A.COD_PRODUTO
		,A.COD_EAN 
		,CADASTRO.DESCRICAO
		,A.COD_FORNECEDOR
		,FORN.DES_FORNECEDOR		
 FROM [192.168.0.6].INTEGRACOES.DBO.[FORNECEDOR_CONSIGNADO] A WITH (NOLOCK)
		 LEFT JOIN BI.DBO.BI_CAD_PRODUTO CADASTRO WITH (NOLOCK)
				ON A.COD_PRODUTO = CADASTRO.COD_PRODUTO
		LEFT JOIN INTEGRACOES.DBO.TAB_FORNECEDOR FORN WITH (NOLOCK)
				ON FORN.COD_FORNECEDOR = A.COD_FORNECEDOR
 ";
		r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
		r
];
(*t\[LeftArrow]getProdutosEXCEL[$lojas];*)


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridTRAVAS[r],grid8[r2]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];

rep=marcheTemplate[grid=Row@{"Status Fornecedores com Troca e Consignados em ",$now},createReport[s],1200, $logo];


$fileName1="Dash Status TROCA e CONSIGNADOS em "<>$now2<>".png";
Export["Dash Status TROCA e CONSIGNADOS em "<>$now2<>".png",rep];


ClearAll@createXLSXReport;
createXLSXReport[]:=Module[{subject,body,tables,dates,intervalType,tempFile,destFile,r,r3,r6,newFile,resp},
	
	tempFile=FileNameJoin@{mrtFileDirectory[],"Trocas_e_Consignados.xlsx"};
	$destFile=FileNameJoin@{mrtFileDirectory[],"reports","Trocas_e_Consignados_"<>$now2<>".xlsx"};
	
	r3\[LeftArrow]getEXCEL[$lojas];
	r6\[LeftArrow]getProdutosEXCEL[$lojas];

	Check[
		mrtUpdateExcelTemplate[
		 tempFile
		,$destFile
		,{ 
                <|"Sheet" -> "Fornecedores", "TableBase" -> "Forn" , "Data" -> r3["Data"]|>
               ,<|"Sheet" -> "Produtos", "TableBase" -> "Prod" , "Data" -> r6["Data"]|>
		 }
		];
		,$destFile=$Failed
	];
	
	$destFile
]

createXLSXReport[];


marcheMail[
  		"[GE] Status Fornecedores com Troca e Consignados em " <>$now 
  		,""
	      ,$mailsGerencia
	      ,{$fileName1,$destFile}	  
] 
