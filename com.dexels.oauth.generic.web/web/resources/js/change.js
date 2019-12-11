$( function() {
    var GETSObject = GETS();
    if( typeof GETSObject[ "userId"] == "undefined" || typeof GETSObject[ "activationId" ] == "undefined" ) {
        displayAlert( "danger", "Fout", "Er is niet goed gegaan met het laden van de pagina, probeer het later opnieuw" );
    }
    
    $.getJSON( "/oauth/descriptionendpoint?client_id=" + GETSObject['client_id'], function( data ) {
        $( function() {
            if (typeof data != 'undefined' && typeof data.description != 'undefined')
                $("#club").text(data.description);
        } );
    });
    
    $(  '#change-form' ).submit( function( event ) {
        event.preventDefault();
        var password = $( '#change-password' ).val(); 
        var passwordRepeat = $( '#change-password-repeat' ).val();
        
        if( !password ) {
            displayAlert( 'danger', '', "Vul alle velden in" );
            return;
        }
        
        if( password != passwordRepeat ) {
            displayAlert( 'danger', '', "Wachtwoorden zijn niet gelijk" );
            return;
        }
                
        var GETSObject = GETS();
        var parameters = {
                password: password,
                userId: GETSObject[ "userId" ],
                activationId: GETSObject[ "activationId" ]
        };
        request( '/oauth/changeendpoint', parameters, 'change-button' , '#change-form');
    } );
} );
