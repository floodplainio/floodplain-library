var email_regex = /^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}$/i;

function handleLoginStatus(data) {
	$(".clientdescription").html(data.clientdescription);
    localStorage.setItem('clientdescription',data.clientdescription);

    if(data.errorheader || data.error) {
    	displayAlert('danger', data.errorheader, data.error);
    }
}

function GETS() {
    var search = decodeURIComponent( document.location.search );
    if( search.length == 0 )
        return {};
    
    //Removes the question mark
    if( search.charAt( 0 ) == "?" ) 
        search = search.substring( 1 );
    
    var results = {};
    $( search.split( "&" ) ).each( function () {
        var key = $( this.split( "=" ) )[ 0 ];
        var value = $( this.split( "=" ) )[ 1 ];
        
        //If we split and we've more than 2 items it is probably the base64 encoded string so we 
        //need to add a = because we split removed it
        if ( this.split( "=" ).length > 2 ) {
            value += "=";
        }

        results[ key ] = encodeURIComponent( value );
    } );
    return results;
}

function shouldPrevent() {
    var args = Array.prototype.slice.call(arguments);
    var list = $(args).map(function() {
        return !this || this.length == 0;
    });
    return $.inArray(true, list) > -1;
}

function request(service, parameters, spinnerId, hideOnSuccessElemId) {
    var animate = (function () {
        if(!spinnerId) 
            return function() {};
        
        var spinner = Ladda.create(document.getElementById(spinnerId));
        return function(state) {
            if(state) 
                spinner.start();
            else
                spinner.stop();
        }
    })();
    
    var components = [];
    $.each(parameters, function(key, value) {
        components.push(key + "=" + value);
    });
    
    animate(true);
    $.getJSON(service + "?" + components.join('&'), function(data) {
        if(data.isSuccessful) {
            displayAlert('success', data.title, data.message);
            if (hideOnSuccessElemId) {
            	$(hideOnSuccessElemId).hide();
            }
            
        } else {
            displayAlert('danger', data.title, data.message);
        }
        animate(false);
    });
}	

function checkCookie() {
    var cookieEnabled=(navigator.cookieEnabled)? true : false;
    if (typeof navigator.cookieEnabled=="undefined" && !cookieEnabled){ 
        document.cookie="testcookie";
        cookieEnabled=(document.cookie.indexOf("testcookie")!=-1)? true : false;
    }
    return cookieEnabled
}

function hideAlert() {
    $( '#popup' ).hide();
    
}

function displayAlert( type, title, message ) {
   
        $( '#popup' ).removeClass( 'alert-danger alert-success alert-warning alert-info' );
        $( '#popup' ).addClass( 'alert-' + type );
        $( '#popup' ).show(0);

        $( '#title' ).text( title );
        $( '#message' ).text( message );			
    
}

