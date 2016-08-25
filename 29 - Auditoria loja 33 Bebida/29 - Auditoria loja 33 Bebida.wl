(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$lojas={33};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getALIMENTUM[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
	
		SET NOCOUNT ON;	
SELECT 
		COD_LOJA Loja
		,CONVERT(VARCHAR,DTA_ENTRADA,103) [Dta Entrada]
		,E.COD_PRODUTO Plu
		,DESCRICAO [Descri\[CCedilla]\[ATilde]o]
		,NO_DEPARTAMENTO Depto
		,QTD_ENTRADA*QTD_EMBALAGEM [Qtde Entrada]
		,DES_FORNECEDOR Forn
		,NO_COMPRADOR Comprador
FROM  [192.168.0.6].[ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] E
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO AS CP
			ON CP.COD_PRODUTO = E.COD_PRODUTO
		INNER JOIN BI.DBO.COMPRAS_CAD_COMPRADOR AS CC
			ON 1=1
		AND CP.COD_USUARIO = CC.COD_USUARIO
WHERE 1=1
		AND DTA_ENTRADA >= CONVERT(DATE,GETDATE()-7)
		AND COD_LOJA IN (`1`)
		AND E.COD_PRODUTO IN (SELECT DISTINCT COD_PRODUTO FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE WHERE 1=1 AND COD_AJUSTE IN (281,283) AND COD_LOJA = 29 )
		AND E.COD_PRODUTO NOT IN (01030730,01030731,00049566)
		AND E.COD_FORNECEDOR NOT IN (102856)		

		ORDER BY CONVERT(DATE,DTA_ENTRADA) DESC , QTD_ENTRADA*QTD_EMBALAGEM  DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r\[LeftArrow]getALIMENTUM[$lojas]


grid8[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<0,Red
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Qtde Entrada"}-> stl,{"Loja"}-> "N"},"TotalRow"->False];
	title=Style[Row@{"Top Itens"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid8[r];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{grid8[r]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];
rep=marcheTemplate[grid=Row@{"Auditoria Entrada 33-Restaurante ",$now},createReport[s],1200, $logo];


$fileName1="Auditoria NF Venda em "<>$now2<>".png";
Export["Auditoria NF Venda em "<>$now2<>".png",rep];


marcheMail[
  		"[GE] Auditoria de entrada Bebida nos ultimos 7 dias " <>$now 
  		,"Todas as entradas deste itens devem ser efetivadas na loja 29-Eataly"
		  ,$mailsGerencia
		  ,$fileName1
]
