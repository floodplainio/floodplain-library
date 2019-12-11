var lastUpdate = new Date();
	
function timeDifference(current, previous) {
    var msPerMinute = 60 * 1000;
    var msPerHour = msPerMinute * 60;
    var msPerDay = msPerHour * 24;
    var msPerMonth = msPerDay * 30;
    var msPerYear = msPerDay * 365;

    var elapsed = current - previous;

    if (elapsed < msPerMinute) {
         return Math.round(elapsed/1000) + ' seconds ago';   
    }

    else if (elapsed < msPerHour) {
         return Math.round(elapsed/msPerMinute) + ' minutes ago';   
    }

    else if (elapsed < msPerDay ) {
         return Math.round(elapsed/msPerHour ) + ' hours ago';   
    }

    else if (elapsed < msPerMonth) {
        return 'approximately ' + Math.round(elapsed/msPerDay) + ' days ago';   
    }

    else if (elapsed < msPerYear) {
        return 'approximately ' + Math.round(elapsed/msPerMonth) + ' months ago';   
    }

    else {
        return 'approximately ' + Math.round(elapsed/msPerYear ) + ' years ago';   
    }
}

$(document).ready(
    function() {
        Handlebars.registerHelper('isHidden', function(statusvalue, text, options) {
            if (arguments.length < 3)
                throw new Error("Handlebars Helper isHidden needs 2 parameters");
            if (sessionStorage.getItem(text) === null) {
                var result = false;
            } else {
                var result = sessionStorage.getItem(text) === "hidden";
            }
            if (result) {
                return options.fn(this);
            } else {
                return options.inverse(this);
            }
        });
        
        Handlebars.registerHelper('isRestarting', function(restart, deployment, cluster) {
        	if(restart == "true" && deployment == cluster)
        		return new Handlebars.SafeString('<div class="column"><div class="loader"></div></div><div class="column">&nbsp;&nbsp;Please wait. Restart is in process... </div>');
        	else if (restart =="unknown")
        		return '<div class="column-10">Something is wrong with this cluster</div>';
    	});

        updateShortStatus();
        updateStatus();
        updateAlerts();
        updateTopics();
        
        setInterval(function() {
            updateStatus();
            updateShortStatus();
            updateAlerts();
            updateTopics();
        }, 10000);
        
        setInterval(function() {
            var now = new Date();
            if ( (now - lastUpdate) > 21000 ) {
                $('#lastUpdate').show();
                $('#lastUpdateTime').text(timeDifference(new Date, lastUpdate));
            } else {
                $('#lastUpdate').hide();
            }
        }, 1000);
   
        $('#taboverview').click(function(e) {
        	$('#shortstatus').show();
        	$('#status').hide();
        	$('#alerts').hide();
        	$('#replication').hide();
        });

        $('#tabstatus').click(function(e) {
        	$('#shortstatus').hide();
        	$('#status').show();
        	$('#alerts').hide();
        	$('#replication').hide();
        });

        $('#tabalerts').click(function(e) {
        	$('#shortstatus').hide();
        	$('#status').hide();
        	$('#alerts').show();
        	$('#replication').hide();
        });
        $('#tabreplication').click(function(e) {
        	$('#shortstatus').hide();
        	$('#status').hide();
        	$('#alerts').hide();
        	$('#replication').show();
        });        
        
    });

function updateShortStatus() {
	
	 $.getJSON("/monitorapi?query=shortstatus", function(data) {
		 var problems = "<h2> Overview </h2>";
		 var problemCount = 0;
		 $.each(data.groups, function(key, result) {
			 if (result.length > 0) {
				 problemCount += 1;
				 problems += "<h4>" + key + "</h4>";
				 problems += "<ul>";
				 $.each(result, function(idx, failure) {
					 problems += "<li>" + failure + "</li>" 
				 });
				 problems += "</ul>"
			 }
		 });
		 if (problemCount == 0) {
    		 problems += "<p>No problems</p>";
		 }
		 $("#shortstatus").html(problems);
	 });
}

function updateStatus() {
    var source = $("#content-template").html();
    var template = Handlebars.compile(source);
    $.getJSON("/monitorapi?query=globalstatus", function(data) {
        
        lastUpdate = new Date();
        $.each(data.groups, function(index, group) {
            group.clusters.sort(function(a, b) {
            	return a.name.localeCompare(b.name);
        	});
            $.each(group.clusters, function(index, cluster) {
                cluster.servers.sort(function(a, b) {
                    return a.name.localeCompare(b.name);
            	});
                $.each(cluster.servers, function(index, server) {
                	console.log(server)
                    server.checkResults.sort(function(a, b) {
                        return a.checkName.localeCompare(b.checkName);
                	});
                });
                
            });
        });

        $("#status").html(template(data));

        $('li').click(function(e) {
        	//if(e.target == e.currentTarget){
        		$(this).children('.plus').text('-');
                var wasHidden = $(this).children('ul:hidden').length === 0
                if (wasHidden) {
                    $(this).children('.plus').text('+');
                } else {
                    $(this).children('.plus').text('-');
                }
                var text = $(this).clone().children().remove().end().text().trim();
                $(this).children('ul').slideToggle(400, function() {
                    if (!wasHidden) {
                        window.sessionStorage.setItem(text, "visible");
                    } else {
                        window.sessionStorage.setItem(text, "hidden");
                    }
                });
                e.stopPropagation();
        	//}
        });
        
        $( ".rbutton" ).click(function(e) {
        	var timeout = prompt("Please define the timeout parameter for the restart","300")
        	var cluster = $(this).attr('id').replace("-btn","");
        	var namespace = $(this).closest('li.namespace').attr('id').replace('-namespace','');
        	var pods = '';
        	$(this).closest('ul.Clusters').find('li.Server').each(function(){
        		if($(this).attr('id').includes(cluster)){
            		var str = $(this).attr('id');
            		var middle = Math.ceil(str.length / 2);
            	    var s1 = str.slice(0, middle);
            	    if(s1.substr(s1.length - 1) == '-'){
            	    	s1 = s1.slice(0, s1.length - 1);
            	    }
            		pods = pods + s1 + ';'
        		}
        	});
        	if(pods.substr(pods.length - 1) == ';'){
        		pods = pods.slice(0, pods.length - 1);
    	    }
        	if (timeout) {
        		if(confirm("Are you sure you want to perform a rolling restart?")){
        			var url = "/monitorapi?query=restart&cluster="+cluster+"&timeout="+timeout+"&namespace="+namespace+"&pods="+pods;
        			$.getJSON(url, function(data) {
        	        	restartCluster = "";
        	        });
        		}
        	}
        	e.stopPropagation();
    	});
        
        $(".deletegeneration").click(function(e) {
        	console.log("aap!");
        	alert("Monkey!");
        });
    });
    
   
};
function updateAlerts() {
    var source = $("#alerts-template").html();
    var template = Handlebars.compile(source);
    $.getJSON("/monitorapi?query=alerts", function(data) {
        $("#alerts").html(template(data));
    });
};

function updateTopics() {
	console.log("topics!");
    var source = $("#replication-template").html();
    var template = Handlebars.compile(source);
    $.getJSON("/kafka?list=true", function(data) {
        $("#replication").html(template(data));
    });
}