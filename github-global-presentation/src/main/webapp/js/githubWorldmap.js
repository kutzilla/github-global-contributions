d3.select(window).on("resize", throttle);

var zoom = d3.behavior.zoom().scaleExtent([ 1, 100 ]).on("zoom", move);

var width = document.getElementById('container').offsetWidth;
var height = width / 2;

var topo, projection, path, svg, g;
var country;
var allCountries, allCities;

var gesamtAmountCommits = 0;
var maxAmountCommitsCountry = 0;
var maxAmountCommitsCity = 0;

var cityPoints = {};
var numberAmountTexts = {};

var allSvgCountries = {};

var graticule = d3.geo.graticule();

var tooltip = d3.select("#container").append("div").attr("class",
		"tooltip hidden");

setup(width, height);

window.setInterval(dataReload, 5000);


function setup(width, height) {
	
	projection = d3.geo.mercator().translate([ (width / 2), (height / 2) ])
			.scale(width / 2 / Math.PI);
	path = d3.geo.path().projection(projection);
	svg = d3.select("#container").append("svg").attr("width", width).attr(
			"height", height).call(zoom).append("g");
	g = svg.append("g");
	
	
}

d3.json("data/world-topo-min.json", function(error, world) {
	var countries = topojson.feature(world, world.objects.countries).features;
	topo = countries;
	initDraw(topo, countries);
	
	var urlRepos = 'http://localhost:8090/global-github-contributions/rest/json/github/getAllRepos';
	$.getJSON(urlRepos, function(repos) {
		loadComboRepo(repos);
		dataReload();
	});
	
});

function userPressedLoad(){
	dataReload();
}

function dataReload(){
	var repoChooser = document.getElementById('repochooser');
//	if(repoChooser.size > 0){	
		var repo = repoChooser.options[repoChooser.selectedIndex].value;
		var von = document.getElementById('from').value;
		var bis = document.getElementById('to').value;
		var urlHBaseData = "http://localhost:8090/global-github-contributions/rest/json/github/getAllCommitsData?repo="+repo+"&from="+von+"&to="+bis;
		//urlHBaseData = "http://localhost:8090/global-github-contributions/rest/json/github/getAllCommitsData";
		var urlCountries = "http://localhost:8080/global-github-contributions/rest/json/github/getAllCommitsOfAllCountries?repo="+repo+"&from="+von+"&to="+bis;
		var urlCities = "http://localhost:8080/global-github-contributions/rest/json/github/getAllCommitsOfAllCities?repo="+repo+"&from="+von+"&to="+bis;
		$.getJSON(urlHBaseData, function(data) { 
			console.log("neueDaten: cities:"+data.cities.length+", countries: "+data.countries.length); 
			var cities = data.cities;
			var countries = data.countries; 
			gesamtAmountCommits = data.gesamtAmountCommits;
			maxAmountCommitsCountry = data.maxAmountCommitsCountry;
			maxAmountCommitsCity = data.maxAmountCommitsCity;
			allCountries = countries;
			allCities = cities;
			$("#lblRepository").html(repo);
			$("#lblAmountAllCommits").html(gesamtAmountCommits);
			
			continueDraw(countries); 
			for (var cityPoint in cityPoints) {
				$(".citytooltip"+cityPoint).remove(); 
			}
			drawCities(topo, cities);
		});
}

function loadComboRepo(repos){
	var sel = document.getElementById('repochooser');
	for(var i=sel.options.length-1; i>=0; i--){
		sel.remove(i);
    }
	for(var i = 0; i < repos.length; i++) {
	    var opt = document.createElement('option');
	    opt.innerHTML = repos[i].repo;
	    opt.value = repos[i].repo;
	    sel.appendChild(opt);
	}
	if(sel.options.length > 0){
		$('#repochooser').val(sel.options[0].value);
		$('#repochooser').selectmenu("refresh");
	}
}
function getColorForCountryString(countryName, countries) {
//	console.log ( 'getColorForCountryString'+countryName+'; '+countries.length );
	var r = 216;
	var g = 220;
	var b = 224;
	if(countries != null){
		for (var i = 0; i < countries.length; i++) {
			var aktCountry = countries[i];
			if (countryName == aktCountry.country) {
				var percent = aktCountry.amount / maxAmountCommitsCountry;
				var newR = parseInt(r - (percent * 216));
				var newG = parseInt(g - (percent * 153));
				var newB = parseInt(b - (percent * 81));
//				return rgbToHex(newR, newG, newB);
				r = newR;
				g = newG;
				b = newB; 
				return "rgb("+r+", "+g+", "+b+")";
			}
		}
	}
	return "rgb("+r+", "+g+", "+b+")";
}

function getColorForCountry(countryName, countries) {
	var r = 216;
	var g = 220;
	var b = 224;
	if(countries != null){
		for (var i = 0; i < countries.length; i++) {
			var aktCountry = countries[i];
			if (countryName === aktCountry.country) {
				var percent = aktCountry.amount / maxAmountCommitsCountry;
				var newR = parseInt(r - (percent * 216));
				var newG = parseInt(g - (percent * 153));
				var newB = parseInt(b - (percent * 81));
				return rgbToHex(newR, newG, newB);
			}
		}
	}
	return rgbToHex(r, g, b);
}

function drawCities(topo, cities) {
	cities.forEach(function(city) {
		drawCityPoints(city.latitude, city.longitude, city.city, city.amount);
	});
}

function continueDraw(countries){
//	 console.log ( 'continueDraw' );
	 
	 
//	var countries = g.selectAll(".country");
	var allCountries = $(".country");
	for(var i=0; i<allCountries.length; i++){
		var aktCountry = allCountries[i];
		var countryName = aktCountry.getAttribute("title");
//		console.log ( 'zeig her das country: '+aktCountry.html );
		var gelb = getColorForCountryString(countryName, countries);
		aktCountry.style.fill = gelb;
		aktCountry.onmouseover = function (evt) {
//			console.log(evt.target);
//			if(evt.srcElement != undefined){		
				var cName = evt.target.getAttribute("title");
				
				var amount = getCommitsAmountCountry(cName);
				$("#lblLocation").html(cName);
				$("#lblAmountCommits").html(amount);
//			}
		}
		aktCountry.onmouseout = function (evt) {
			$("#lblLocation").html("");
			$("#lblAmountCommits").html("");
		}
		aktCountry.onclick = function(evt){
			$(".modal-body").html("");
			var cName = evt.target.getAttribute("title");
			var amount = getCommitsAmountCountry(cName);
			$(".modal-title").html(cName);
			var users = getUsersOfCountry(cName);
			if(users != null){
				var html = getDiaglogHtml(users);
				$(".modal-body").html(html);
			}
			var repoChooser = document.getElementById('repochooser');
			var repo = repoChooser.options[repoChooser.selectedIndex].value;
			$(".modal-repo").html(repo);
			$(".modal-commits").html(amount);
			$("#myModal").modal();
		};

	}
	
}

function getDiaglogHtml(users){
	$(".modal-body").html("");
	var html = "<div style='overflow:scroll; height:400px;'>";
	html +="<table>";
	for(var i=0; i< users.length; i++){
		html+="<tr><td>";
		$(".modal-body").append("<p>"+users[i].login+"</p>");
//		console.log(users[i]);
		var profilepic = "https://avatars.githubusercontent.com/"+users[i].login;
		var htmlimage = "<img src="+profilepic+" width='50px' height='50px'/>";
		html+= htmlimage;
		html+="</td><td>";
		html+="<a href='https://github.com/"+users[i].login+"' target='_blank'>";
		html+="<h4>"+users[i].login+"</h4>";
		html+="</a>";
		html+="</td></tr>"
	}
	html += "</table></div>"
	return html;
}




function getUsersOfCity(cityName){
	console.log("________________"+this.allCities);
	for (var i = 0; i < this.allCities.length; i++) {
		var aktCity = this.allCities[i]; 
		if(aktCity.city === cityName){
			return aktCity.users;
		}
	}
	return null;
}
function getUsersOfCountry(countryName){
	for (var i = 0; i < this.allCountries.length; i++) {
		var aktCountry = this.allCountries[i];
		if(aktCountry.country === countryName){
			return aktCountry.users;
		}
	}
	return null;
}

function getCommitsAmountCountry(cName){
	var result = "";
	for(var i=0; i<allCountries.length; i++){
		var aktC = allCountries[i];
		if(aktC.country === cName){
			result = aktC.amount;
			return result;
		}
	}
	return 0;
}
function getCommitsAmountCity(cName){
	var result = "";
	for(var i=0; i<allCities.length; i++){
		var aktC = allCities[i];
		if(aktC.city === cName){
			result = aktC.amount;
			return result;
		}
	}
	return 0;
}

function initDraw(topo, countries) {
//	alert("initDraw");
	svg.append("path").datum(graticule).attr("class", "graticule").attr("d",
			path);

	g.append("path").datum(
			{
				type : "LineString",
				coordinates : [ [ -180, 0 ], [ -90, 0 ], [ 0, 0 ], [ 90, 0 ],
						[ 180, 0 ] ]
			}).attr("d", path);

	country = g.selectAll(".country").data(topo);
	
	
	country.enter().insert("path").attr("class", "country").attr("d", path)
			.attr("id", function(d, i) {
				return d.id;
			}).attr("title", function(d, i) {
				allSvgCountries[d.properties.name] = country;
				return d.properties.name;
			}).style("fill", function(d, i) {
				var gelb = getColorForCountry(d.properties.name, countries);
				return gelb;
			});
	
	// offsets for tooltips
	var offsetL = document.getElementById('container').offsetLeft + 20;
	var offsetT = document.getElementById('container').offsetTop + 10;

}

 

function drawCityPoints(lat, lon, city, amount) {
//	console.log(city +"_");
	var x = projection([ lat, lon ])[0];
	var y = projection([ lat, lon ])[1];
	var aktCityPoint = cityPoints[city];
	if(aktCityPoint != null ){
		aktCityPoint.remove();
	}	
	aktCityPoint = g.append("g").attr("class", "gpoint");
	
	var percent = amount / maxAmountCommitsCity;
	var relAmount = percent * 15 ;
	
	
	
	cityPoints[city] = aktCityPoint;
	var r = relAmount / (2 * Math.PI);

	aktCityPoint.append("svg:circle")
		.attr("cx", x)
		.attr("cy", y)
		.attr("title", city+"") 
		.attr("fill","red")
		.attr("fill-opacity",0.7)
		.attr("class","citytooltip"+city)
		.attr("r", r);
	/*
	$(".citytooltip"+city).qtip({ 
	    content: {
	        text: city
	    } 
	});*/ 
	
	$(".citytooltip"+city).mouseover(function(evt) {
	  var cName = evt.currentTarget.getAttribute("title");
	  var cAmount = getCommitsAmountCity(cName);
	  $("#lblLocation").html(cName);
	  $("#lblAmountCommits").html(cAmount);
	});
	$(".citytooltip"+city).mouseout(function(evt) {
		$("#lblLocation").html("");
		$("#lblAmountCommits").html("");
	});
	$(".citytooltip"+city).click(function(evt) {
		$(".modal-body").html("");
		var cName = evt.currentTarget.getAttribute("title");
		var amount = getCommitsAmountCity(cName);
		$(".modal-title").html(cName); 
		var users = getUsersOfCity(cName);
		if(users != null){
			var html = getDiaglogHtml(users);
			$(".modal-body").html(html);
		}
		var repoChooser = document.getElementById('repochooser');
		var repo = repoChooser.options[repoChooser.selectedIndex].value;
		$(".modal-repo").html(repo);
		$(".modal-commits").html(amount);
		$("#myModal").modal();
	});
	
}


function addNumberAmountText(lat, lon, text, country) {
	var textPoint = numberAmountTexts[country];
	if(textPoint != null){
		textPoint.remove();
	}
	textPoint = g.append("g").attr("class", "gpoint");
	numberAmountTexts[country] = textPoint;
	var x = projection([ lat, lon ])[0];
	var y = projection([ lat, lon ])[1];
	if (text.length > 0) {
		textPoint.append("text").attr("x", x).attr("y", y).attr("class", "text")
				.text(text);
	}

}
function redraw() {
	width = document.getElementById('container').offsetWidth;
	height = width / 2;
	d3.select('svg').remove();
	setup(width, height);
	dataReload();
	initDraw(topo, allCountries);
}

function move() {

	var t = d3.event.translate;
	var s = d3.event.scale;
	zscale = s;
	var h = height / 4;

	t[0] = Math
			.min((width / height) * (s - 1), Math.max(width * (1 - s), t[0]));

	t[1] = Math.min(h * (s - 1) + h * s, Math.max(height * (1 - s) - h * s,
			t[1]));

	zoom.translate(t);
	g.attr("transform", "translate(" + t + ")scale(" + s + ")");

	// adjust the country hover stroke width based on zoom level
	d3.selectAll(".country").style("stroke-width", 1 / s);

}

function click(d) {
	if (active === d)
		return reset();
	g.selectAll(".active").classed("active", false);
	d3.select(this).classed("active", active = d);

	var b = path.bounds(d);
	g.transition().duration(750).attr(
			"transform",
			"translate("
					+ projection.translate()
					+ ")"
					+ "scale("
					+ .95
					/ Math.max((b[1][0] - b[0][0]) / width, (b[1][1] - b[0][1])
							/ height) + ")" + "translate("
					+ -(b[1][0] + b[0][0]) / 2 + "," + -(b[1][1] + b[0][1]) / 2
					+ ")");
}

var throttleTimer;
function throttle() {
	window.clearTimeout(throttleTimer);
	throttleTimer = window.setTimeout(function() {
		redraw();
	}, 200);
}
/**
function click() {
	var latlon = projection.invert(d3.mouse(this));
	console.log(latlon);
}
*/
function componentToHex(c) {
	var hex = c.toString(16);
	return hex.length == 1 ? "0" + hex : hex;
}

function rgbToHex(r, g, b) {
	return "#" + componentToHex(r) + componentToHex(g) + componentToHex(b);
}