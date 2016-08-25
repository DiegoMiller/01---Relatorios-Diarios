(* ::Package:: *)

(* ::Section:: *)
(*DNV*)


Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


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


$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31};
$deParaLoja=maGetDeParaLojas[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$mails=marcheGetMails[];
$cargosLoja={"GERENTE","COORD_PERECIVEL","COORD_RELACIONAMENTO","COORD_NAO_PERECIVEL","REGIONAL","COORD_PREVENCAO","FISCAL_PREVENCAO"};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];


getDNV[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 

				SET NOCOUNT ON;

				IF OBJECT_ID('TEMPDB..#TMP_METADADOS') IS NOT NULL   DROP TABLE #TMP_METADADOS

				CREATE TABLE #TMP_METADADOS
				(
						COD_PRODUTO INT
						,MTD_MP INT
						,MTD_IMP_PROP INT
						,MTD_IMP INT
						,MTD_MARCA VARCHAR(90)
						,MTD_ORG_PROD    VARCHAR(90)
						,MTD_NOTAVEL INT 
						,MTD_ULTRA_NOTAVEL INT
						,MTD_IMP_PROP_EAT INT
						,MTD_TOP20_RUPTURA INT
						,MTD_PI INT

				)

				INSERT INTO #TMP_METADADOS
				EXEC [192.168.0.13].BI.DBO.QW_LISTA_METADADOS

		
				SELECT		TOP 10 
							NO_LOJA AS LOJA
							,EP.COD_PRODUTO AS PLU
							,DESCRICAO
							,CLASSIF_PRODUTO AS ABC
							,NO_DEPARTAMENTO AS DEPARTAMENTO
							,NO_SECAO AS SE\[CapitalCCedilla]\[CapitalATilde]O
							,CAST(QTD_ESTOQUE AS DOUBLE PRECISION) as ESTOQUE
				FROM
							[192.168.0.13].DW.DBO.ESTOQUE AS EP WITH (NOLOCK) 
							LEFT JOIN [192.168.0.13].[BI].DBO.BI_CAD_PRODUTO AS CP WITH (NOLOCK) ON (CP.COD_PRODUTO	= EP.COD_PRODUTO)
							LEFT JOIN  [192.168.0.13].[BI].[DBO].[BI_CAD_LOJA2] as LJ with (NOLOCK)  ON (EP.COD_LOJA = LJ.COD_LOJA)
							LEFT JOIN #TMP_METADADOS AS MTD WITH (NOLOCK) ON (MTD.COD_PRODUTO = EP.COD_PRODUTO)
				WHERE 1 = 1
							AND CONVERT(DATE,EP.DATA) = CONVERT(DATE,GETDATE())
							AND QTD_ESTOQUE < -1
							AND CLASSIF_PRODUTO IN ('A1','A2','A3')
							AND COD_DEPARTAMENTO  IN (19,10)
							AND EP.COD_PRODUTO NOT IN (543101)
							AND EP.COD_PRODUTO NOT IN (SELECT DISTINCT COD_PRODUTO FROM [192.168.0.13].[BI].[DBO].CADASTRO_CAD_PRODUTO_METADADOS WHERE 1=1 AND COD_METADADO = 42)
							AND MTD_ORG_PROD NOT IN ('LOJA','CENTRAL')
							AND MTD_MP  NOT IN (1)
							AND EP.COD_LOJA  IN (`1`)		
				ORDER BY QTD_ESTOQUE

				DROP TABLE #TMP_METADADOS
	
														
";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r["DateInterval"]={dtIni1,dtFim1};
	r["CodLoja"]=codLoja;
	r["NoLoja"]=codLoja/.$deParaLoja;
	r["FileName"]=ToString@Row@{"Negativos ",r["NoLoja"]," ",$now2,".png"};
	r
]
r[99]=getDNV[$lojas]


i=0;Dynamic@i
Scan[(i++;r[#]=getDNV[#])&,$lojas]


gridDNV[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
				 #>0,Red
			,True,Red
	
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[r,"ColumnFormat"->{{"ESTOQUE"}-> stl},"TotalRow"->False];
	title=Style[Row@{"Negativos ", \!\(\*
StyleBox[
TemplateBox[{"$now"},
"RowDefault"],
StripOnInput->False,
LineColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
FrontFaceColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
BackFaceColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
GraphicsColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
FontFamily->"Helvetica",
FontWeight->Bold,
FontColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`]]\)},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridDNV[r[99]]


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

$fileName=ToString@Row@{"Resumo - Negativos "<>$now2<>".png"};
Export["Resumo - Negativos "<>$now2<>".png",gridDNV[r[99]]];


createFile[codLoja_Integer]:=Module[{},
	Export[r[codLoja]["FileName"],gridDNV[r[codLoja]]]
]


i=0;Dynamic@i
Scan[(i++;createFile[#])&,$lojas[[]]]


ClearAll@sendMails
sendMails[codLoja_Integer,mail_:Automatic]:=Module[{mails},

	mails=If[mail===Automatic,Flatten@$mails[codLoja,$cargosLoja],mail];

	marcheMail[
  		ToString@Row@{"Estoques Negativos ",codLoja/.$deParaLoja,"|",IntegerString[codLoja,10,2]," ",$now}
  		,"Relat\[OAcute]rio de Estoque Negativos

Agenda:
Segunda-Feira   Congelado e Frios
Ter\[CCedilla]a-Feira	 Adega e Bebida
Quarta-Feira	Bazar e Bomboniere
Quinta-Feira	Sazonal, Limpeza e Perfumaria
Sexta-Feira	 Mercearia

Procedimentos que devem ser adotados pelas lojas:
- Verificar exposi\[CCedilla]\[ATilde]o;
- Verificar precifica\[CCedilla]\[ATilde]o;
- Verificar estoque (Caso exista diferen\[CCedilla]a entre o sistema e o f\[IAcute]sico informar o saldo correto atrav\[EAcute]s da Ruptura no Delph).


Para mudan\[CCedilla]a/corre\[CCedilla]\[ATilde]o de emails favor alterar
http://goo.gl/qWZu6x"
  		,mails
  		,{r[codLoja]["FileName"]}
  ]
]
(*sendMails[2,"diego.miller@marche.com.br"]*)


i=0;Dynamic@i
Scan[(i++;sendMails[#])&,$lojas[[]]]


marcheMail[
  		"[GE] Estoques Negativos " <>$now
  		,"Relat\[OAcute]rio de Estoque Negativos

Agenda:
Segunda-Feira   Congelado e Frios
Ter\[CCedilla]a-Feira	 Adega e Bebida
Quarta-Feira	Bazar e Bomboniere
Quinta-Feira	Sazonal, Limpeza e Perfumaria
Sexta-Feira	 Mercearia

Procedimentos que devem ser adotados pelas lojas:
- Verificar exposi\[CCedilla]\[ATilde]o;
- Verificar precifica\[CCedilla]\[ATilde]o;
- Verificar estoque (Caso exista diferen\[CCedilla]a entre o sistema e o f\[IAcute]sico informar o saldo correto atrav\[EAcute]s da Ruptura no Delph)."
  		,$mailsGerencia
  		,{$fileName,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[7]["FileName"],r[9]["FileName"],r[12]["FileName"],r[13]["FileName"],r[17]["FileName"],r[18]["FileName"],r[20]["FileName"],r[21]["FileName"],r[22]["FileName"],r[23]["FileName"],r[24]["FileName"],r[25]["FileName"],r[27]["FileName"],r[29]["FileName"],r[30]["FileName"],r[31]["FileName"]}
  ]


(*(* Teste *)*)
(*i=0;Dynamic@i*)
(*Scan[(i++;sendMails[#,"diego.miller@marche.com.br"])&,$lojas[[;;3]]]*)
