/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
// This script unused
schema = graph.schema();

schema.propertyKey("name").asText().create();
schema.propertyKey("born").asInt().create();
schema.propertyKey("title").asText().create();
schema.propertyKey("released").asInt().create();
schema.propertyKey("score").asInt().create();
schema.propertyKey("roles").asText().create();

schema.vertexLabel("person").properties("name", "born").primaryKeys("name").create();
schema.vertexLabel("movie").properties("title", "released").primaryKeys("title").create();

schema.edgeLabel("ACTED_IN").multiTimes().properties("roles").sourceLabel("person").targetLabel("movie").sortKeys("roles").create();
schema.edgeLabel("DIRECTED").properties("score").sourceLabel("person").targetLabel("movie").create();
schema.edgeLabel("PRODUCED").properties("score").sourceLabel("person").targetLabel("movie").create();
schema.edgeLabel("WROTE").properties("score").sourceLabel("person").targetLabel("movie").create();

schema.indexLabel("personByBorn").onV("person").by("born").range().create();

theMatrix = graph.addVertex(T.label, "movie", "title", "The Matrix", "released", 1999);
keanu = graph.addVertex(T.label, "person", "name", "keanu Reeves", "born", 1964);
carrie = graph.addVertex(T.label, "person", "name", "carrie-anne Moss", "born", 1967);
laurence = graph.addVertex(T.label, "person", "name", "laurence Fishburne", "born", 1961);
hugo = graph.addVertex(T.label, "person", "name", "hugo Weaving", "born", 1960);
lillyW = graph.addVertex(T.label, "person", "name", "Lilly Wachowski", "born", 1967);
lanaW = graph.addVertex(T.label, "person", "name", "Lana Wachowski", "born", 1965);
joelS = graph.addVertex(T.label, "person", "name", "Joel Silver", "born", 1952);

keanu.addEdge("ACTED_IN", theMatrix, "roles", "Neo");
carrie.addEdge("ACTED_IN", theMatrix, "roles", "Trinity");
laurence.addEdge("ACTED_IN", theMatrix, "roles", "Morpheus");
hugo.addEdge("ACTED_IN", theMatrix, "roles", "agent Smith");
lillyW.addEdge("DIRECTED", theMatrix, "score", 10);
lanaW.addEdge("DIRECTED", theMatrix, "score", 10);
joelS.addEdge("PRODUCED", theMatrix, "score", 10);

emil = graph.addVertex(T.label, "person", "name", "emil Eifrem", "born", 1978);
emil.addEdge("ACTED_IN", theMatrix, "roles", "emil");

theMatrixReloaded = graph.addVertex(T.label, "movie", "title", "The Matrix Reloaded", "released", 2003);

keanu.addEdge("ACTED_IN", theMatrixReloaded, "roles", "Neo");
carrie.addEdge("ACTED_IN", theMatrixReloaded, "roles", "Trinity");
laurence.addEdge("ACTED_IN", theMatrixReloaded, "roles", "Morpheus");
hugo.addEdge("ACTED_IN", theMatrixReloaded, "roles", "agent Smith");
lillyW.addEdge("DIRECTED", theMatrixReloaded, "score", 10);
lanaW.addEdge("DIRECTED", theMatrix, "score", 10);
joelS.addEdge("PRODUCED", theMatrixReloaded, "score", 10);

theMatrixRevolutions = graph.addVertex(T.label, "movie", "title", "The Matrix Revolutions", "released", 2003);

keanu.addEdge("ACTED_IN", theMatrixRevolutions, "roles", "Neo");
carrie.addEdge("ACTED_IN", theMatrixRevolutions, "roles", "Trinity");
laurence.addEdge("ACTED_IN", theMatrixRevolutions, "roles", "Morpheus");
hugo.addEdge("ACTED_IN", theMatrixRevolutions, "roles", "agent Smith");
lillyW.addEdge("DIRECTED", theMatrixRevolutions, "score", 10);
lanaW.addEdge("DIRECTED", theMatrixRevolutions, "score", 10);
joelS.addEdge("PRODUCED", theMatrixRevolutions, "score", 10);

theDevilsadvocate = graph.addVertex(T.label, "movie", "title", "The Devil's advocate", "released", 1997);

charlize = graph.addVertex(T.label, "person", "name", "charlize Theron", "born", 1975);
al = graph.addVertex(T.label, "person", "name", "al Pacino", "born", 1940);
taylor = graph.addVertex(T.label, "person", "name", "taylor Hackford", "born", 1944);

keanu.addEdge("ACTED_IN", theDevilsadvocate, "roles", "Kevin Lomax");
charlize.addEdge("ACTED_IN", theDevilsadvocate, "roles", "Mary ann Lomax");
al.addEdge("ACTED_IN", theDevilsadvocate, "roles", "John Milton");
taylor.addEdge("DIRECTED", theDevilsadvocate, "score", 10);

aFewGoodMen = graph.addVertex(T.label, "movie", "title", "a Few Good Men", "released", 1992);

tomC = graph.addVertex(T.label, "person", "name", "Tom Cruise", "born", 1962);
jackN = graph.addVertex(T.label, "person", "name", "Jack Nicholson", "born", 1937);
demiM = graph.addVertex(T.label, "person", "name", "Demi Moore", "born", 1962);
kevinB = graph.addVertex(T.label, "person", "name", "Kevin Bacon", "born", 1958);
kieferS = graph.addVertex(T.label, "person", "name", "Kiefer Sutherland", "born", 1966);
noahW = graph.addVertex(T.label, "person", "name", "Noah Wyle", "born", 1971);
cubaG = graph.addVertex(T.label, "person", "name", "Cuba Gooding Jr.", "born", 1968);
kevinP = graph.addVertex(T.label, "person", "name", "Kevin Pollak", "born", 1957);
jtw = graph.addVertex(T.label, "person", "name", "J.T. Walsh", "born", 1943);
jamesM = graph.addVertex(T.label, "person", "name", "James Marshall", "born", 1967);
christopherG = graph.addVertex(T.label, "person", "name", "Christopher Guest", "born", 1948);
robR = graph.addVertex(T.label, "person", "name", "Rob Reiner", "born", 1947);
aaronS = graph.addVertex(T.label, "person", "name", "aaron Sorkin", "born", 1961);

tomC.addEdge("ACTED_IN", aFewGoodMen, "roles", "Lt. Daniel Kaffee");
jackN.addEdge("ACTED_IN", aFewGoodMen, "roles", "Col. nathan R. Jessup");
demiM.addEdge("ACTED_IN", aFewGoodMen, "roles", "Lt. Cdr. Joanne Galloway");
kevinB.addEdge("ACTED_IN", aFewGoodMen, "roles", "Capt. Jack Ross");
kieferS.addEdge("ACTED_IN", aFewGoodMen, "roles", "Lt. Jonathan Kendrick");
noahW.addEdge("ACTED_IN", aFewGoodMen, "roles", "Cpl. Jeffrey Barnes");
cubaG.addEdge("ACTED_IN", aFewGoodMen, "roles", "Cpl. Carl Hammaker");
kevinP.addEdge("ACTED_IN", aFewGoodMen, "roles", "Lt. Sam Weinberg");
jtw.addEdge("ACTED_IN", aFewGoodMen, "roles", "Lt. Col. Matthew andrew Markinson");
jamesM.addEdge("ACTED_IN", aFewGoodMen, "roles", "Pfc. Louden Downey");
christopherG.addEdge("ACTED_IN", aFewGoodMen, "roles", "Dr. Stone");
aaronS.addEdge("ACTED_IN", aFewGoodMen, "roles", "Man in Bar");
robR.addEdge("DIRECTED", aFewGoodMen, "score", 10);
aaronS.addEdge("WROTE", aFewGoodMen, "score", 10);

topGun = graph.addVertex(T.label, "movie", "title", "Top Gun", "released", 1986);

kellyM = graph.addVertex(T.label, "person", "name", "Kelly McGillis", "born", 1957);
valK = graph.addVertex(T.label, "person", "name", "Val Kilmer", "born", 1959);
anthonyE = graph.addVertex(T.label, "person", "name", "anthony Edwards", "born", 1962);
tomS = graph.addVertex(T.label, "person", "name", "Tom Skerritt", "born", 1933);
megR = graph.addVertex(T.label, "person", "name", "Meg Ryan", "born", 1961);
tonyS = graph.addVertex(T.label, "person", "name", "Tony Scott", "born", 1944);
jimC = graph.addVertex(T.label, "person", "name", "Jim Cash", "born", 1941);

tomC.addEdge("ACTED_IN", topGun, "roles", "Maverick");
kellyM.addEdge("ACTED_IN", topGun, "roles", "Charlie");
valK.addEdge("ACTED_IN", topGun, "roles", "Iceman");
anthonyE.addEdge("ACTED_IN", topGun, "roles", "Goose");
tomS.addEdge("ACTED_IN", topGun, "roles", "Viper");
megR.addEdge("ACTED_IN", topGun, "roles", "Carole");
tonyS.addEdge("DIRECTED", topGun, "score", 10);
jimC.addEdge("WROTE", topGun, "score", 10);

jerryMaguire = graph.addVertex(T.label, "movie", "title", "Jerry Maguire", "released", 2000);

reneeZ = graph.addVertex(T.label, "person", "name", "Renee Zellweger", "born", 1969);
kellyP = graph.addVertex(T.label, "person", "name", "Kelly Preston", "born", 1962);
jerryO = graph.addVertex(T.label, "person", "name", "Jerry O'Connell", "born", 1974);
jayM = graph.addVertex(T.label, "person", "name", "Jay Mohr", "born", 1970);
bonnieH = graph.addVertex(T.label, "person", "name", "Bonnie Hunt", "born", 1970);
reginaK = graph.addVertex(T.label, "person", "name", "Regina King", "born", 1961);
jonathanL = graph.addVertex(T.label, "person", "name", "Jonathan Lipnicki", "born", 1996);
cameronC = graph.addVertex(T.label, "person", "name", "Cameron Crowe", "born", 1957);

tomC.addEdge("ACTED_IN", jerryMaguire, "roles", "Jerry Maguire");
cubaG.addEdge("ACTED_IN", jerryMaguire, "roles", "Rod Tidwell");
reneeZ.addEdge("ACTED_IN", jerryMaguire, "roles", "Dorothy Boyd");
kellyP.addEdge("ACTED_IN", jerryMaguire, "roles", "avery Bishop");
jerryO.addEdge("ACTED_IN", jerryMaguire, "roles", "Frank Cushman");
jayM.addEdge("ACTED_IN", jerryMaguire, "roles", "Bob Sugar");
bonnieH.addEdge("ACTED_IN", jerryMaguire, "roles", "Laurel Boyd");
reginaK.addEdge("ACTED_IN", jerryMaguire, "roles", "Marcee Tidwell");
jonathanL.addEdge("ACTED_IN", jerryMaguire, "roles", "Ray Boyd");
cameronC.addEdge("DIRECTED", jerryMaguire, "score", 10);
cameronC.addEdge("PRODUCED", jerryMaguire, "score", 10);
cameronC.addEdge("WROTE", jerryMaguire, "score", 10);

standByMe = graph.addVertex(T.label, "movie", "title", "Stand By Me", "released", 1986);

riverP = graph.addVertex(T.label, "person", "name", "River Phoenix", "born", 1970);
coreyF = graph.addVertex(T.label, "person", "name", "Corey Feldman", "born", 1971);
wilW = graph.addVertex(T.label, "person", "name", "Wil Wheaton", "born", 1972);
johnC = graph.addVertex(T.label, "person", "name", "John Cusack", "born", 1966);
marshallB = graph.addVertex(T.label, "person", "name", "Marshall Bell", "born", 1942);

wilW.addEdge("ACTED_IN", standByMe, "roles", "Gordie Lachance");
riverP.addEdge("ACTED_IN", standByMe, "roles", "Chris Chambers");
jerryO.addEdge("ACTED_IN", standByMe, "roles", "Vern Tessio");
coreyF.addEdge("ACTED_IN", standByMe, "roles", "Teddy Duchamp");
johnC.addEdge("ACTED_IN", standByMe, "roles", "Denny Lachance");
kieferS.addEdge("ACTED_IN", standByMe, "roles", "ace Merrill");
marshallB.addEdge("ACTED_IN", standByMe, "roles", "Mr. Lachance");
robR.addEdge("DIRECTED", standByMe, "score", 10);

asGoodasItGets = graph.addVertex(T.label, "movie", "title", "as Good as It Gets", "released", 1997);

helenH = graph.addVertex(T.label, "person", "name", "Helen Hunt", "born", 1963);
gregK = graph.addVertex(T.label, "person", "name", "Greg Kinnear", "born", 1963);
jamesB = graph.addVertex(T.label, "person", "name", "James L. Brooks", "born", 1940);

jackN.addEdge("ACTED_IN", asGoodasItGets, "roles", "Melvin Udall");
helenH.addEdge("ACTED_IN", asGoodasItGets, "roles", "Carol Connelly");
gregK.addEdge("ACTED_IN", asGoodasItGets, "roles", "Simon Bishop");
cubaG.addEdge("ACTED_IN", asGoodasItGets, "roles", "Frank Sachs");
jamesB.addEdge("DIRECTED", asGoodasItGets, "score", 10);

whatDreamsMayCome = graph.addVertex(T.label, "movie", "title", "What Dreams May Come", "released", 1998);

annabellaS = graph.addVertex(T.label, "person", "name", "annabella Sciorra", "born", 1960);
maxS = graph.addVertex(T.label, "person", "name", "Max von Sydow", "born", 1929);
wernerH = graph.addVertex(T.label, "person", "name", "Werner Herzog", "born", 1942);
robin = graph.addVertex(T.label, "person", "name", "robin Williams", "born", 1951);
vincentW = graph.addVertex(T.label, "person", "name", "Vincent Ward", "born", 1956);

robin.addEdge("ACTED_IN", whatDreamsMayCome, "roles", "Chris Nielsen");
cubaG.addEdge("ACTED_IN", whatDreamsMayCome, "roles", "albert Lewis");
annabellaS.addEdge("ACTED_IN", whatDreamsMayCome, "roles", "annie Collins-Nielsen");
maxS.addEdge("ACTED_IN", whatDreamsMayCome, "roles", "The Tracker");
wernerH.addEdge("ACTED_IN", whatDreamsMayCome, "roles", "The Face");
vincentW.addEdge("DIRECTED", whatDreamsMayCome, "score", 10);

snowFallingonCedars = graph.addVertex(T.label, "movie", "title", "Snow Falling on Cedars", "released", 1999);

ethanH = graph.addVertex(T.label, "person", "name", "Ethan Hawke", "born", 1970);
rickY = graph.addVertex(T.label, "person", "name", "Rick Yune", "born", 1971);
jamesC = graph.addVertex(T.label, "person", "name", "James Cromwell", "born", 1940);
scottH = graph.addVertex(T.label, "person", "name", "Scott Hicks", "born", 1953);

ethanH.addEdge("ACTED_IN", snowFallingonCedars, "roles", "Ishmael Chambers");
rickY.addEdge("ACTED_IN", snowFallingonCedars, "roles", "Kazuo Miyamoto");
maxS.addEdge("ACTED_IN", snowFallingonCedars, "roles", "Nels Gudmundsson");
jamesC.addEdge("ACTED_IN", snowFallingonCedars, "roles", "Judge Fielding");
scottH.addEdge("DIRECTED", snowFallingonCedars, "score", 10);

youveGotMail = graph.addVertex(T.label, "movie", "title", "You've Got Mail", "released", 1998);

parkerP = graph.addVertex(T.label, "person", "name", "Parker Posey", "born", 1968);
daveC = graph.addVertex(T.label, "person", "name", "Dave Chappelle", "born", 1973);
steveZ = graph.addVertex(T.label, "person", "name", "Steve Zahn", "born", 1967);
tomH = graph.addVertex(T.label, "person", "name", "Tom Hanks", "born", 1956);
noraE = graph.addVertex(T.label, "person", "name", "Nora Ephron", "born", 1941);

tomH.addEdge("ACTED_IN", youveGotMail, "roles", "Joe Fox");
megR.addEdge("ACTED_IN", youveGotMail, "roles", "Kathleen Kelly");
gregK.addEdge("ACTED_IN", youveGotMail, "roles", "Frank Navasky");
parkerP.addEdge("ACTED_IN", youveGotMail, "roles", "Patricia Eden");
daveC.addEdge("ACTED_IN", youveGotMail, "roles", "Kevin Jackson");
steveZ.addEdge("ACTED_IN", youveGotMail, "roles", "George Pappas");
noraE.addEdge("DIRECTED", youveGotMail, "score", 10);

sleeplessInSeattle = graph.addVertex(T.label, "movie", "title", "Sleepless in Seattle", "released", 1993);

ritaW = graph.addVertex(T.label, "person", "name", "Rita Wilson", "born", 1956);
billPull = graph.addVertex(T.label, "person", "name", "Bill Pullman", "born", 1953);
victorG = graph.addVertex(T.label, "person", "name", "Victor Garber", "born", 1949);
rosieO = graph.addVertex(T.label, "person", "name", "Rosie O'Donnell", "born", 1962);

tomH.addEdge("ACTED_IN", sleeplessInSeattle, "roles", "Sam Baldwin");
megR.addEdge("ACTED_IN", sleeplessInSeattle, "roles", "annie Reed");
ritaW.addEdge("ACTED_IN", sleeplessInSeattle, "roles", "Suzy");
billPull.addEdge("ACTED_IN", sleeplessInSeattle, "roles", "Walter");
victorG.addEdge("ACTED_IN", sleeplessInSeattle, "roles", "Greg");
rosieO.addEdge("ACTED_IN", sleeplessInSeattle, "roles", "Becky");
noraE.addEdge("DIRECTED", sleeplessInSeattle, "score", 10);

joeVersustheVolcano = graph.addVertex(T.label, "movie", "title", "Joe Versus the Volcano", "released", 1990);

johnS = graph.addVertex(T.label, "person", "name", "John Patrick Stanley", "born", 1950);
nathan = graph.addVertex(T.label, "person", "name", "nathan Lane", "born", 1956);

tomH.addEdge("ACTED_IN", joeVersustheVolcano, "roles", "Joe Banks");
megR.addEdge("ACTED_IN", joeVersustheVolcano, "roles", "DeDe, angelica Graynamore, Patricia Graynamore");
nathan.addEdge("ACTED_IN", joeVersustheVolcano, "roles", "Baw");
johnS.addEdge("DIRECTED", joeVersustheVolcano, "score", 10);

whenHarryMetSally = graph.addVertex(T.label, "movie", "title", "When Harry Met Sally", "released", 1998);

billyC = graph.addVertex(T.label, "person", "name", "Billy Crystal", "born", 1948);
carrieF = graph.addVertex(T.label, "person", "name", "carrie Fisher", "born", 1956);
brunoK = graph.addVertex(T.label, "person", "name", "Bruno Kirby", "born", 1949);

billyC.addEdge("ACTED_IN", whenHarryMetSally, "roles", "Harry Burns");
megR.addEdge("ACTED_IN", whenHarryMetSally, "roles", "Sally albright");
carrieF.addEdge("ACTED_IN", whenHarryMetSally, "roles", "Marie");
brunoK.addEdge("ACTED_IN", whenHarryMetSally, "roles", "Jess");
robR.addEdge("DIRECTED", whenHarryMetSally, "score", 10);
robR.addEdge("PRODUCED", whenHarryMetSally, "score", 10);
noraE.addEdge("PRODUCED", whenHarryMetSally, "score", 10);
noraE.addEdge("WROTE", whenHarryMetSally, "score", 10);

thatThingYouDo = graph.addVertex(T.label, "movie", "title", "That Thing You Do", "released", 1996);

livT = graph.addVertex(T.label, "person", "name", "Liv Tyler", "born", 1977);

tomH.addEdge("ACTED_IN", thatThingYouDo, "roles", "Mr. White");
livT.addEdge("ACTED_IN", thatThingYouDo, "roles", "Faye Dolan");
charlize.addEdge("ACTED_IN", thatThingYouDo, "roles", "Tina");
tomH.addEdge("DIRECTED", thatThingYouDo, "score", 10);

theReplacements = graph.addVertex(T.label, "movie", "title", "The Replacements", "released", 2000);

brooke = graph.addVertex(T.label, "person", "name", "brooke Langton", "born", 1970);
gene = graph.addVertex(T.label, "person", "name", "gene Hackman", "born", 1930);
orlando = graph.addVertex(T.label, "person", "name", "orlando Jones", "born", 1968);
howard = graph.addVertex(T.label, "person", "name", "howard Deutch", "born", 1950);

keanu.addEdge("ACTED_IN", theReplacements, "roles", "Shane Falco");
brooke.addEdge("ACTED_IN", theReplacements, "roles", "annabelle Farrell");
gene.addEdge("ACTED_IN", theReplacements, "roles", "Jimmy McGinty");
orlando.addEdge("ACTED_IN", theReplacements, "roles", "Clifford Franklin");
howard.addEdge("DIRECTED", theReplacements, "score", 10);

rescueDawn = graph.addVertex(T.label, "movie", "title", "rescueDawn", "released", 2006);

christianB = graph.addVertex(T.label, "person", "name", "Christian Bale", "born", 1974);
zachG = graph.addVertex(T.label, "person", "name", "Zach Grenier", "born", 1954);

marshallB.addEdge("ACTED_IN", rescueDawn, "roles", "admiral");
christianB.addEdge("ACTED_IN", rescueDawn, "roles", "Dieter Dengler");
zachG.addEdge("ACTED_IN", rescueDawn, "roles", "Squad Leader");
steveZ.addEdge("ACTED_IN", rescueDawn, "roles", "Duane");
wernerH.addEdge("DIRECTED", rescueDawn, "score", 10);

theBirdcage = graph.addVertex(T.label, "movie", "title", "The Birdcage", "released", 1996);

mikeN = graph.addVertex(T.label, "person", "name", "Mike Nichols", "born", 1931);

robin.addEdge("ACTED_IN", theBirdcage, "roles", "armand Goldman");
nathan.addEdge("ACTED_IN", theBirdcage, "roles", "albert Goldman");
gene.addEdge("ACTED_IN", theBirdcage, "roles", "Sen. Kevin Keeley");
mikeN.addEdge("DIRECTED", theBirdcage, "score", 10);

unforgiven = graph.addVertex(T.label, "movie", "title", "unforgiven", "released", 1992);

richardH = graph.addVertex(T.label, "person", "name", "Richard Harris", "born", 1930);
clintE = graph.addVertex(T.label, "person", "name", "Richard Harris", "born", 1930);

richardH.addEdge("ACTED_IN", unforgiven, "roles", "English Bob");
clintE.addEdge("ACTED_IN", unforgiven, "roles", "Bill Munny");
gene.addEdge("ACTED_IN", unforgiven, "roles", "Little Bill Daggett");
clintE.addEdge("DIRECTED", unforgiven, "score", 10);

johnnyMnemonic = graph.addVertex(T.label, "movie", "title", "Johnny Mnemonic", "released", 1995);

takeshi = graph.addVertex(T.label, "person", "name", "takeshi Kitano", "born", 1947);
dina = graph.addVertex(T.label, "person", "name", "dina Meyer", "born", 1968);
iceT = graph.addVertex(T.label, "person", "name", "Ice-T", "born", 1958);
robertL = graph.addVertex(T.label, "person", "name", "Robert Longo", "born", 1953);

keanu.addEdge("ACTED_IN", johnnyMnemonic, "roles", "Johnny Mnemonic");
takeshi.addEdge("ACTED_IN", johnnyMnemonic, "roles", "Takahashi");
dina.addEdge("ACTED_IN", johnnyMnemonic, "roles", "Jane");
iceT.addEdge("ACTED_IN", johnnyMnemonic, "roles", "J-Bone");
robertL.addEdge("DIRECTED", johnnyMnemonic, "score", 10);

cloudatlas = graph.addVertex(T.label, "movie", "title", "Cloud atlas", "released", 2012);

halleB = graph.addVertex(T.label, "person", "name", "Halle Berry", "born", 1966);
jimB = graph.addVertex(T.label, "person", "name", "Jim Broadbent", "born", 1949);
tomT = graph.addVertex(T.label, "person", "name", "Tom Tykwer", "born", 1965);
davidMitchell = graph.addVertex(T.label, "person", "name", "David Mitchell", "born", 1969);
stefanarndt = graph.addVertex(T.label, "person", "name", "Stefan arndt", "born", 1961);

tomH.addEdge("ACTED_IN", cloudatlas, "roles", "Zachry, Dr. Henry Goose, Isaac Sachs, Dermot Hoggins");
hugo.addEdge("ACTED_IN", cloudatlas, "roles", "Bill Smoke, Haskell Moore, Tadeusz Kesselring, Nurse Noakes, Boardman Mephi, Old Georgie");
halleB.addEdge("ACTED_IN", cloudatlas, "roles", "Luisa Rey, Jocasta ayrs, Ovid, Meronym");
jimB.addEdge("ACTED_IN", cloudatlas, "roles", "Vyvyan ayrs, Captain Molyneux, Timothy Cavendish");
tomT.addEdge("DIRECTED", cloudatlas, "score", 10);
lillyW.addEdge("DIRECTED", cloudatlas, "score", 10);
lanaW.addEdge("DIRECTED", cloudatlas, "score", 10);
davidMitchell.addEdge("WROTE", cloudatlas, "score", 10);
stefanarndt.addEdge("PRODUCED", cloudatlas, "score", 10);

theDaVinciCode = graph.addVertex(T.label, "movie", "title", "The Da Vinci Code", "released", 2006);

ianM = graph.addVertex(T.label, "person", "name", "Ian McKellen", "born", 1939);
audreyT = graph.addVertex(T.label, "person", "name", "audrey Tautou", "born", 1976);
paulB = graph.addVertex(T.label, "person", "name", "Paul Bettany", "born", 1971);
ronH = graph.addVertex(T.label, "person", "name", "Ron howard", "born", 1954);

tomH.addEdge("ACTED_IN", theDaVinciCode, "roles", "Dr. Robert Langdon");
ianM.addEdge("ACTED_IN", theDaVinciCode, "roles", "Sir Leight Teabing");
audreyT.addEdge("ACTED_IN", theDaVinciCode, "roles", "Sophie Neveu");
paulB.addEdge("ACTED_IN", theDaVinciCode, "roles", "Silas");
ronH.addEdge("DIRECTED", theDaVinciCode, "score", 10);

vforVendetta = graph.addVertex(T.label, "movie", "title", "The Da Vinci Code", "released", 2006);

natalieP = graph.addVertex(T.label, "person", "name", "Natalie Portman", "born", 1981);
stephenR = graph.addVertex(T.label, "person", "name", "Stephen Rea", "born", 1946);
johnH = graph.addVertex(T.label, "person", "name", "John Hurt", "born", 1940);
benM = graph.addVertex(T.label, "person", "name", "Ben Miles", "born", 1967);

hugo.addEdge("ACTED_IN", vforVendetta, "roles", "V");
natalieP.addEdge("ACTED_IN", vforVendetta, "roles", "Evey Hammond");
stephenR.addEdge("ACTED_IN", vforVendetta, "roles", "Eric Finch");
johnH.addEdge("ACTED_IN", vforVendetta, "roles", "High Chancellor adam Sutler");
benM.addEdge("ACTED_IN", vforVendetta, "roles", "Dascomb");
jamesM.addEdge("DIRECTED", vforVendetta, "score", 10);
lillyW.addEdge("PRODUCED", vforVendetta, "score", 10);
lanaW.addEdge("PRODUCED", vforVendetta, "score", 10);
joelS.addEdge("PRODUCED", vforVendetta, "score", 10);
lillyW.addEdge("WROTE", vforVendetta, "score", 10);
lanaW.addEdge("WROTE", vforVendetta, "score", 10);

speedRacer = graph.addVertex(T.label, "movie", "title", "Speed Racer", "released", 2008);

matthewF = graph.addVertex(T.label, "person", "name", "Matthew Fox", "born", 1966);
emileH = graph.addVertex(T.label, "person", "name", "Emile Hirsch", "born", 1985);
johnG = graph.addVertex(T.label, "person", "name", "John Goodman", "born", 1940);
susanS = graph.addVertex(T.label, "person", "name", "Susan Sarandon", "born", 1966);
christinaR = graph.addVertex(T.label, "person", "name", "Christina Ricci", "born", 1980);
rain = graph.addVertex(T.label, "person", "name", "Rain", "born", 1982);

emileH.addEdge("ACTED_IN", speedRacer, "roles", "Speed Racer");
johnG.addEdge("ACTED_IN", speedRacer, "roles", "Pops");
susanS.addEdge("ACTED_IN", speedRacer, "roles", "Mom");
matthewF.addEdge("ACTED_IN", speedRacer, "roles", "Racer X");
christinaR.addEdge("ACTED_IN", speedRacer, "roles", "Trixie");
rain.addEdge("ACTED_IN", speedRacer, "roles", "Taejo Togokahn");
benM.addEdge("ACTED_IN", speedRacer, "roles", "Kass Jones");
lillyW.addEdge("DIRECTED", speedRacer, "score", 10);
lanaW.addEdge("DIRECTED", speedRacer, "score", 10);
lillyW.addEdge("WROTE", speedRacer, "score", 10);
lanaW.addEdge("WROTE", speedRacer, "score", 10);
joelS.addEdge("PRODUCED", speedRacer, "score", 10);

ninjaassassin = graph.addVertex(T.label, "movie", "title", "Speed Racer", "released", 2009);

naomieH = graph.addVertex(T.label, "person", "name", "Naomie Harris", "born", 1982);

rain.addEdge("ACTED_IN", ninjaassassin, "roles", "Raizo");
naomieH.addEdge("ACTED_IN", ninjaassassin, "roles", "Mika Coretti");
rickY.addEdge("ACTED_IN", ninjaassassin, "roles", "takeshi");
benM.addEdge("ACTED_IN", ninjaassassin, "roles", "Ryan Maslow");
jamesM.addEdge("DIRECTED", ninjaassassin, "score", 10);
lillyW.addEdge("PRODUCED", ninjaassassin, "score", 10);
lanaW.addEdge("PRODUCED", ninjaassassin, "score", 10);
joelS.addEdge("PRODUCED", ninjaassassin, "score", 10);

theGreenMile = graph.addVertex(T.label, "movie", "title", "The Green Mile", "released", 1999);

michaelD = graph.addVertex(T.label, "person", "name", "Michael Clarke Duncan", "born", 1957);
davidM = graph.addVertex(T.label, "person", "name", "David Morse", "born", 1953);
samR = graph.addVertex(T.label, "person", "name", "Sam Rockwell", "born", 1968);
garyS = graph.addVertex(T.label, "person", "name", "Gary Sinise", "born", 1955);
patriciaC = graph.addVertex(T.label, "person", "name", "Patricia Clarkson", "born", 1959);
frankD = graph.addVertex(T.label, "person", "name", "Frank Darabont", "born", 1959);

tomH.addEdge("ACTED_IN", theGreenMile, "roles", "Paul Edgecomb");
michaelD.addEdge("ACTED_IN", theGreenMile, "roles", "John Coffey");
davidM.addEdge("ACTED_IN", theGreenMile, "roles", "Brutus");
bonnieH.addEdge("ACTED_IN", theGreenMile, "roles", "Jan Edgecomb");
jamesC.addEdge("ACTED_IN", theGreenMile, "roles", "Warden Hal Moores");
samR.addEdge("ACTED_IN", theGreenMile, "roles", "Wild Bill' Wharton");
garyS.addEdge("ACTED_IN", theGreenMile, "roles", "Burt Hammersmith");
patriciaC.addEdge("ACTED_IN", theGreenMile, "roles", "Melinda Moores");
frankD.addEdge("DIRECTED", theGreenMile, "score", 10);

frostNixon = graph.addVertex(T.label, "movie", "title", "Frost/Nixon", "released", 2008);

frankL = graph.addVertex(T.label, "person", "name", "Frank Langella", "born", 1938);
michaelS = graph.addVertex(T.label, "person", "name", "Michael Sheen", "born", 1969);
oliverP = graph.addVertex(T.label, "person", "name", "Oliver Platt", "born", 1960);

frankL.addEdge("ACTED_IN", frostNixon, "roles", "Richard Nixon");
michaelS.addEdge("ACTED_IN", frostNixon, "roles", "David Frost");
kevinB.addEdge("ACTED_IN", frostNixon, "roles", "Jack Brennan");
oliverP.addEdge("ACTED_IN", frostNixon, "roles", "Bob Zelnick");
samR.addEdge("ACTED_IN", frostNixon, "roles", "James Reston, Jr.");
ronH.addEdge("DIRECTED", frostNixon, "score", 10);

hoffa = graph.addVertex(T.label, "movie", "title", "hoffa", "released", 1992);

dannyD = graph.addVertex(T.label, "person", "name", "Danny DeVito", "born", 1944);
johnR = graph.addVertex(T.label, "person", "name", "John C. Reilly", "born", 1965);

jackN.addEdge("ACTED_IN", hoffa, "roles", "hoffa");
dannyD.addEdge("ACTED_IN", hoffa, "roles", "Robert Ciaro");
jtw.addEdge("ACTED_IN", hoffa, "roles", "Frank Fitzsimmons");
johnR.addEdge("ACTED_IN", hoffa, "roles", "Peter Connelly");
dannyD.addEdge("DIRECTED", hoffa, "score", 10);

apollo13 = graph.addVertex(T.label, "movie", "title", "apollo 13", "released", 1995);

edH = graph.addVertex(T.label, "person", "name", "Ed Harris", "born", 1950);
billPax = graph.addVertex(T.label, "person", "name", "Bill Paxton", "born", 1955);

tomH.addEdge("ACTED_IN", apollo13, "roles", "Jim Lovell");
kevinB.addEdge("ACTED_IN", apollo13, "roles", "Jack Swigert");
edH.addEdge("ACTED_IN", apollo13, "roles", "gene Kranz");
billPax.addEdge("ACTED_IN", apollo13, "roles", "Fred Haise");
garyS.addEdge("ACTED_IN", apollo13, "roles", "Ken Mattingly");
ronH.addEdge("DIRECTED", apollo13, "score", 10);

twister = graph.addVertex(T.label, "movie", "title", "twister", "released", 1996);

philipH = graph.addVertex(T.label, "person", "name", "Philip Seymour Hoffman", "born", 1967);
janB = graph.addVertex(T.label, "person", "name", "Jan de Bont", "born", 1943);

billPax.addEdge("ACTED_IN", twister, "roles", "Bill Harding");
helenH.addEdge("ACTED_IN", twister, "roles", "Dr. Jo Harding");
zachG.addEdge("ACTED_IN", twister, "roles", "Eddie");
philipH.addEdge("ACTED_IN", twister, "roles", "Dustin 'Dusty' Davis");
janB.addEdge("DIRECTED", twister, "score", 10);

castaway = graph.addVertex(T.label, "movie", "title", "Cast away", "released", 2000);

robertZ = graph.addVertex(T.label, "person", "name", "Robert Zemeckis", "born", 1951);

tomH.addEdge("ACTED_IN", castaway, "roles", "Chuck Noland");
helenH.addEdge("ACTED_IN", castaway, "roles", "Kelly Frears");
robertZ.addEdge("DIRECTED", castaway, "score", 10);

oneFlewOvertheCuckoosNest = graph.addVertex(T.label, "movie", "title", "One Flew Over the Cuckoo's Nest", "released", 1975);

milosF = graph.addVertex(T.label, "person", "name", "Milos Forman", "born", 1932);

jackN.addEdge("ACTED_IN", oneFlewOvertheCuckoosNest, "roles", "Randle McMurphy");
dannyD.addEdge("ACTED_IN", oneFlewOvertheCuckoosNest, "roles", "Martini");
milosF.addEdge("DIRECTED", oneFlewOvertheCuckoosNest, "score", 10);

somethingsGottaGive = graph.addVertex(T.label, "movie", "title", "Something's Gotta Give", "released", 2003);

dianeK = graph.addVertex(T.label, "person", "name", "Diane Keaton", "born", 1946);
nancyM = graph.addVertex(T.label, "person", "name", "Nancy Meyers", "born", 1949);

jackN.addEdge("ACTED_IN", somethingsGottaGive, "roles", "Harry Sanborn");
dianeK.addEdge("ACTED_IN", somethingsGottaGive, "roles", "Erica Barry");
keanu.addEdge("ACTED_IN", somethingsGottaGive, "roles", "Julian Mercer");
nancyM.addEdge("DIRECTED", somethingsGottaGive, "score", 10);
nancyM.addEdge("PRODUCED", somethingsGottaGive, "score", 10);
nancyM.addEdge("WROTE", somethingsGottaGive, "score", 10);

bicentennialMan = graph.addVertex(T.label, "movie", "title", "Bicentennial Man", "released", 2000);

chrisC = graph.addVertex(T.label, "person", "name", "Chris Columbus", "born", 1958);

robin.addEdge("ACTED_IN", bicentennialMan, "roles", "andrew Marin");
oliverP.addEdge("ACTED_IN", bicentennialMan, "roles", "Rupert Burns");
chrisC.addEdge("DIRECTED", bicentennialMan, "score", 10);

charlieWilsonsWar = graph.addVertex(T.label, "movie", "title", "Charlie Wilson's War", "released", 2007);

juliaR = graph.addVertex(T.label, "person", "name", "Julia Roberts", "born", 1967);

tomH.addEdge("ACTED_IN", charlieWilsonsWar, "roles", "Rep. Charlie Wilson");
juliaR.addEdge("ACTED_IN", charlieWilsonsWar, "roles", "Joanne Herring");
philipH.addEdge("ACTED_IN", charlieWilsonsWar, "roles", "Gust avrakotos");
mikeN.addEdge("DIRECTED", charlieWilsonsWar, "score", 10);

thePolarExpress = graph.addVertex(T.label, "movie", "title", "The Polar Express", "released", 2004);

tomH.addEdge("ACTED_IN", thePolarExpress, "roles", "Hero Boy");

robertZ.addEdge("DIRECTED", thePolarExpress, "score", 10);

aLeagueofTheirOwn = graph.addVertex(T.label, "movie", "title", "a League of Their Own", "released", 1992);

madonna = graph.addVertex(T.label, "person", "name", "madonna", "born", 1954);
geenaD = graph.addVertex(T.label, "person", "name", "Geena Davis", "born", 1956);
loriP = graph.addVertex(T.label, "person", "name", "Lori Petty", "born", 1963);
pennyM = graph.addVertex(T.label, "person", "name", "Penny Marshall", "born", 1943);

tomH.addEdge("ACTED_IN", aLeagueofTheirOwn, "roles", "Jimmy Dugan");
geenaD.addEdge("ACTED_IN", aLeagueofTheirOwn, "roles", "Dottie Hinson");
loriP.addEdge("ACTED_IN", aLeagueofTheirOwn, "roles", "Kit Keller");
rosieO.addEdge("ACTED_IN", aLeagueofTheirOwn, "roles", "Doris Murphy");
madonna.addEdge("ACTED_IN", aLeagueofTheirOwn, "roles", "all the Way' Mae Mordabito");
billPax.addEdge("ACTED_IN", aLeagueofTheirOwn, "roles", "Bob Hinson");
pennyM.addEdge("DIRECTED", aLeagueofTheirOwn, "score", 10);
