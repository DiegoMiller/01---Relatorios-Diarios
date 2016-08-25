(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]

SetDirectory@mrtFileDirectory[];
$lojas={13};
$mailsGerencia=marcheGetMailFromXML[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$now3=DateString[DatePlus[-1],{"Day","/","Month","/","Year"}];


maGetDeParaLojas[]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql="
		SELECT  [COD_LOJA]
			  ,[NO_LOJA]
		FROM [BI].[dbo].[BI_CAD_LOJA2]
	";
	r\[LeftArrow]mrtSQLDataObject[conn,sql];
	Rule@@@r["Data"]
]
maGetDeParaLojas[];


getEstoqueatual[codLoja_]:=Module[{sql,e,conn=marcheConn2[],tab},
	sql=" SET NOCOUNT ON;	
		
SELECT 					
		NO_LOJA													AS Loja
		,CONVERT(VARCHAR,DATA,103)								 AS Data
		,CONVERT(TIME(0),DTA_GRAVACAO)								AS Horario
		,PM.COD_PRODUTO											AS Plu
		,DESCRICAO												 AS Descricao
		,NO_DEPARTAMENTO										   AS Departamento	
		,CLASSIF_PRODUTO                                           AS ABC
		,QTD_ESTOQUE											  AS 'Qtde Estoque'
			
		FROM DW.DBO.ESTOQUE AS PM
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO AS CAD
		ON PM.COD_PRODUTO = CAD.COD_PRODUTO
		LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2] AS LJ WITH (NOLOCK) 
		ON LJ.COD_LOJA = PM.COD_LOJA

		WHERE 1=1	

		AND PM.DATA = CONVERT(DATE,GETDATE())
		AND PM.COD_LOJA IN (`1`)
		AND PM.COD_PRODUTO IN (492416,283144,248297,143165,495240,703475,287913,688888,61995,915984,464406,73134)

		ORDER BY QTD_ESTOQUE DESC
			
";
		r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
		r["CodLoja"]=codLoja;
		r
];
e\[LeftArrow]getEstoqueatual[$lojas];


(*(*e["DataAll"]//mrtCopyTable2ClipBoard*)*)


gridEstoque[data_Symbol]:=Module[{grid,title,e,color2,stl},
	e\[LeftArrow]data;
	color2=Which[ 
			#<0.04,Darker@Green
			,#<0.06,Orange,
			,True,Red
	]&;

	stl[val_]:=Style[maF["%"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[e,"ColumnFormat"->{{"Qtde Estoque"}-> "N"},"TotalRow"->False];
	title=Style[Row@{"Estoque em ",$now3},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridEstoque[e]


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};
$fileName1="Auditoria diaria.png";
Export["Auditoria diaria.png",gridEstoque[e]];


marcheMail[
  		"Auditoria di\[AAcute]ria de Cervejas - Pav\[ATilde]o " <>$now 
		  ,"Procedimentos que devem ser adotados pelas \[AAcute]reas:\n \nPreven\[CCedilla]\[ATilde]o de Perdas\n- Confer\[EHat]ncia do estoque F\[CapitalIAcute]SICO x SISTEMA, qualquer diferen\[CCedilla]a comunicar o departamento Gest\[ATilde]o de Estoque);\n- Auditoria 100% das entregas da CRBS e SPAL.\n \nGest\[ATilde]o de Estoque\nTem at\[EAcute] 48 horas para analisar as diferen\[CCedilla]as e passar o direcionamento caso necess\[AAcute]rio para a Preven\[CCedilla]\[ATilde]o de Perdas, Seguran\[CCedilla]a Patrimonial e Gerente da Loja.\n \nSeguran\[CCedilla]a patrimonial\n-A partir da analise de Gest\[ATilde]o de Estoque (caso exista diferen\[CCedilla]a n\[ATilde]o justific\[AAcute]vel) dever\[AAcute] verificar atrav\[EAcute]s do monitoramento todas entradas f\[IAcute]sicas dos produtos listados na semana em quest\[ATilde]o."
		  ,$mailsGerencia
		  ,{$fileName1}
];
