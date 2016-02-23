d3.select(window).on("resize", throttle);

var zoom = d3.behavior.zoom().scaleExtent([ 1, 100 ]).on("zoom", move);

var width = document.getElementById('container').offsetWidth;
var height = width / 2;

var topo, projection, path, svg, g;
var country;
var allCountries;

var maxCommitsAmount = 0;

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
			"height", height).call(zoom).on("click", click).append("g");
	g = svg.append("g");
	
	
}

d3.json("data/world-topo-min.json", function(error, world) {
	var countries = topojson.feature(world, world.objects.countries).features;
	topo = countries;
	initDraw(topo, countries);
	
	var urlRepos = 'http://localhost:8080/global-github-contributions/rest/json/github/getAllRepos';
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
	var repo = repoChooser.options[repoChooser.selectedIndex].value;
	var von = document.getElementById('from').value;
	var bis = document.getElementById('to').value;
	var urlHBaseData = "http://localhost:8090/global-github-contributions/rest/json/github/getAllCommitsData?repo="+repo+"&from="+von+"&to="+bis;
	var urlCountries = "http://localhost:8080/global-github-contributions/rest/json/github/getAllCommitsOfAllCountries?repo="+repo+"&from="+von+"&to="+bis;
	var urlCities = "http://localhost:8080/global-github-contributions/rest/json/github/getAllCommitsOfAllCities?repo="+repo+"&from="+von+"&to="+bis;
	$.getJSON(urlHBaseData, function(data) {
//		console.log(data.cities); 
		var cities = data.cities;
		var countries = data.countries;
		for (var i = 0; i < countries.length; i++) {
			if (countries[i].amount > maxCommitsAmount) {
				maxCommitsAmount = countries[i].amount;
			}
		}
		allCountries = countries;
		continueDraw(countries);
		drawCities(topo, cities);
	});
	/**
	$.getJSON(urlCountries, function(countries) {
		for (var i = 0; i < countries.length; i++) {
			if (countries[i].amount > maxCommitsAmount) {
				maxCommitsAmount = countries[i].amount;
			}
		}
		
		continueDraw(countries);
		
		$.getJSON(urlCities, function(cities) {
			drawCities(topo, cities);
		});
	});
	*/
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
				var percent = aktCountry.amount / maxCommitsAmount;
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
				var percent = aktCountry.amount / maxCommitsAmount;
				var newR = parseInt(r - (percent * 216));
				var newG = parseInt(g - (percent * 153));
				var newB = parseInt(b - (percent * 81));
				return rgbToHex(newR, newG, newB);
			}
		}
	}
	return rgbToHex(r, g, b);
}

function drawCities(topo, countries) {
	countries.forEach(function(city) {
		drawCityPoints(city.latitude, city.longitude, city.city, city.amount);
	});
}

function continueDraw(countries){
//	 console.log ( 'continueDraw' );
	 
	 
//	var countries = g.selectAll(".country");
	var allCountries = $(".country")
	for(var i=0; i<allCountries.length; i++){
		var aktCountry = allCountries[i];
		var countryName = aktCountry.getAttribute("title");
//		console.log ( 'zeig her das country: '+aktCountry.html );
		var gelb = getColorForCountryString(countryName, countries);
		aktCountry.style.fill = gelb;
		
	}
	
	/**
	countries.forEach(function(country) {
		addNumberAmountText(country.latitude, country.longitude, country.amount + "", country.country);
	});
	*/ 
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

	// tooltips
	country.on(
			"mousemove",
			function(d, i) {

				var mouse = d3.mouse(svg.node()).map(function(d) {
					return parseInt(d);
				});

				tooltip.classed("hidden", false).attr(
						"style",
						"left:" + (mouse[0] + offsetL) + "px;top:"
								+ (mouse[1] + offsetT) + "px").html(
						d.properties.name);

			}).on("mouseout", function(d, i) {
		tooltip.classed("hidden", true);
	});

}



function drawCityPoints(lat, lon, city, amount) {
	
	var x = projection([ lat, lon ])[0];
	var y = projection([ lat, lon ])[1];
	var aktCityPoint = cityPoints[city];
	if(aktCityPoint != null ){
		aktCityPoint.remove();
	}	
	var relAmount = amount / 10;
	aktCityPoint = g.append("g").attr("class", "gpoint");
	
	cityPoints[city] = aktCityPoint;
	var r = relAmount / (2 * Math.PI);
	aktCityPoint.append("svg:circle")
		.attr("cx", x)
		.attr("cy", y)
		.attr("fill","red")
		.attr("fill-opacity",0.7)
		.attr("class","citytooltip"+city)
		.attr("r", r);
	
	$(".citytooltip"+city).qtip({ 
	    content: {
	        text: city
	    }
	})
	
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
	// draw(topo);
	dataReload();
	initDraw(topo, allCountries);
	/*
	$.getJSON('http://localhost:8080/global-github-contributions/rest/json/github/getAllCommitsOfAllCountries',
		function(data) {
			var countries = data;
			initDraw(topo, countries);
	});
	*/
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

function click() {
	var latlon = projection.invert(d3.mouse(this));
	console.log(latlon);
}
function componentToHex(c) {
	var hex = c.toString(16);
	return hex.length == 1 ? "0" + hex : hex;
}

function rgbToHex(r, g, b) {
	return "#" + componentToHex(r) + componentToHex(g) + componentToHex(b);
}